from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.engine_config import EngineConfig


class EngineConfigRepository(BaseRepository[EngineConfig]):
    """
    Repository for EngineConfig with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(EngineConfig, session)
    
    async def find_all(self) -> List[EngineConfig]:
        """
        Find all engine configurations.
        
        Returns:
            List of all engine configurations
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def find_by_engine_name(self, engine_name: str) -> Optional[EngineConfig]:
        """
        Find an engine configuration by engine name.
        
        Args:
            engine_name: Engine name to search for
            
        Returns:
            EngineConfig if found, else None
        """
        query = select(self.model).where(self.model.engine_name == engine_name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_by_engine_and_key(self, engine_name: str, config_key: str) -> Optional[EngineConfig]:
        """
        Find an engine configuration by engine name and config key.
        
        Args:
            engine_name: Engine name to search for
            config_key: Configuration key to search for
            
        Returns:
            EngineConfig if found, else None
        """
        query = select(self.model).where(
            self.model.engine_name == engine_name,
            self.model.config_key == config_key
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_enabled_configs(self) -> List[EngineConfig]:
        """
        Find all enabled engine configurations.
        
        Returns:
            List of enabled engine configurations
        """
        query = select(self.model).where(self.model.enabled.is_(True))
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def find_by_engine_type(self, engine_type: str) -> List[EngineConfig]:
        """
        Find all engine configurations by engine type.
        
        Args:
            engine_type: Engine type to search for
            
        Returns:
            List of engine configurations
        """
        query = select(self.model).where(self.model.engine_type == engine_type)
        result = await self.session.execute(query)
        return list(result.scalars().all())
        
    async def toggle_enabled(self, engine_name: str, enabled: bool) -> bool:
        """
        Toggle the enabled status for an engine configuration.
        
        Args:
            engine_name: Name of the engine to toggle
            enabled: New enabled status
            
        Returns:
            True if the engine was found and updated, False otherwise
        """
        try:
            # Get the engine config first to check if it exists
            config = await self.find_by_engine_name(engine_name)
            if not config:
                return False
                
            # Update the config attributes
            config.enabled = enabled
            
            # Commit the changes
            await self.session.commit()
            
            # Return success
            return True
                
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in toggle_enabled for {engine_name}: {str(e)}")
            await self.session.rollback()
            raise
    
    async def update_config_value(self, engine_name: str, config_key: str, config_value: str) -> bool:
        """
        Update the configuration value for a specific engine and key.
        
        Args:
            engine_name: Name of the engine
            config_key: Configuration key
            config_value: New configuration value
            
        Returns:
            True if the config was found and updated, False otherwise
        """
        try:
            # Get the engine config first to check if it exists
            config = await self.find_by_engine_and_key(engine_name, config_key)
            if not config:
                return False
                
            # Update the config value
            config.config_value = config_value
            
            # Commit the changes
            await self.session.commit()
            
            # Return success
            return True
                
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in update_config_value for {engine_name}.{config_key}: {str(e)}")
            await self.session.rollback()
            raise
    
    async def get_crewai_flow_enabled(self) -> bool:
        """
        Get the CrewAI flow enabled status.
        
        Returns:
            True if flow is enabled, False otherwise (defaults to True if not found)
        """
        config = await self.find_by_engine_and_key("crewai", "flow_enabled")
        if not config:
            return True  # Default to enabled if not configured
        return config.config_value.lower() == "true"
    
    async def set_crewai_flow_enabled(self, enabled: bool) -> bool:
        """
        Set the CrewAI flow enabled status.
        
        Args:
            enabled: Whether flow should be enabled
            
        Returns:
            True if successful
        """
        config_value = "true" if enabled else "false"
        
        # Try to update existing config first
        success = await self.update_config_value("crewai", "flow_enabled", config_value)
        
        if not success:
            # Create new config if it doesn't exist
            try:
                new_config_data = {
                    "engine_name": "crewai",
                    "engine_type": "workflow",
                    "config_key": "flow_enabled",
                    "config_value": config_value,
                    "enabled": True,
                    "description": "Controls whether CrewAI flow feature is enabled"
                }
                await self.create(new_config_data)
                return True
            except Exception as e:
                import logging
                logging.error(f"Error creating CrewAI flow config: {str(e)}")
                await self.session.rollback()
                raise
        
        return success 