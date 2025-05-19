from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.model_config import ModelConfig


class ModelConfigRepository(BaseRepository[ModelConfig]):
    """
    Repository for ModelConfig with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ModelConfig, session)
    
    async def find_all(self) -> List[ModelConfig]:
        """
        Find all model configurations.
        
        Returns:
            List of all model configurations
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def find_by_key(self, key: str) -> Optional[ModelConfig]:
        """
        Find a model configuration by key.
        
        Args:
            key: Model configuration key to search for
            
        Returns:
            ModelConfig if found, else None
        """
        query = select(self.model).where(self.model.key == key)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_enabled_models(self) -> List[ModelConfig]:
        """
        Find all enabled model configurations.
        
        Returns:
            List of enabled model configurations
        """
        query = select(self.model).where(self.model.enabled.is_(True))
        result = await self.session.execute(query)
        return list(result.scalars().all())
        
    async def toggle_enabled(self, key: str, enabled: bool) -> bool:
        """
        Toggle the enabled status directly with a single DML operation.
        
        Args:
            key: Key of the model to toggle
            enabled: New enabled status
            
        Returns:
            True if the model was found and updated, False otherwise
        """
        try:
            # Get the model first to check if it exists
            model = await self.find_by_key(key)
            if not model:
                return False
                
            # Update the model attributes
            model.enabled = enabled
            
            # Commit the changes
            await self.session.commit()
            
            # Return success
            return True
                
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in toggle_enabled for {key}: {str(e)}")
            await self.session.rollback()
            raise
                    
    async def enable_all_models(self) -> bool:
        """
        Enable all model configurations with a single DML operation.
        
        Returns:
            True if the operation was successful
        """
        try:
            # Use SQLAlchemy update with proper boolean values
            stmt = update(self.model).where(self.model.enabled == False).values(enabled=True)
            await self.session.execute(stmt)
            
            # Commit the changes
            await self.session.commit()
            
            # Return success
            return True
                
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in enable_all_models: {str(e)}")
            await self.session.rollback()
            raise
                    
    async def disable_all_models(self) -> bool:
        """
        Disable all model configurations with a single DML operation.
        
        Returns:
            True if the operation was successful
        """
        try:
            # Use SQLAlchemy update with proper boolean values
            stmt = update(self.model).where(self.model.enabled == True).values(enabled=False)
            await self.session.execute(stmt)
            
            # Commit the changes
            await self.session.commit()
            
            # Return success
            return True
                
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in disable_all_models: {str(e)}")
            await self.session.rollback()
            raise 
    
    async def delete_by_key(self, key: str) -> bool:
        """
        Delete a model configuration by key.
        
        Args:
            key: Key of the model configuration to delete
            
        Returns:
            True if model was found and deleted, False otherwise
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"Attempting to delete model with key: {key}")
            
            # Find the model first
            model = await self.find_by_key(key)
            if not model:
                logger.warning(f"Model with key {key} not found for deletion")
                return False
            
            model_id = model.id
            logger.info(f"Found model with key {key} (ID: {model_id}), proceeding with deletion")
            
            # Delete the model using session.delete() for proper cascade
            await self.session.delete(model)
            
            # Explicitly flush and commit to ensure transaction is completed
            await self.session.flush()
            await self.session.commit()
            
            # Verify the model was deleted
            verification = await self.find_by_key(key)
            if verification:
                logger.error(f"Model with key {key} still exists after deletion attempt")
                return False
                
            logger.info(f"Successfully deleted model with key {key} (ID: {model_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting model with key {key}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            await self.session.rollback()
            raise