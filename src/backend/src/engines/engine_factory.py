"""
Engine Factory module.

This module provides a factory for creating engine instances
based on the engine type.
"""
import logging
from typing import Dict, Any, Optional, List, Type
from sqlalchemy.ext.asyncio import AsyncSession

from src.engines.base.base_engine_service import BaseEngineService
from src.engines.crewai.crewai_engine_service import CrewAIEngineService

logger = logging.getLogger(__name__)

class EngineFactory:
    """
    Factory for creating engine instances
    
    This factory provides methods to create and manage different
    engine implementations.
    """
    
    # Registry of available engine types
    _registry: Dict[str, Type[BaseEngineService]] = {
        "crewai": CrewAIEngineService,
    }
    
    # Cache of engine instances
    _instances: Dict[str, BaseEngineService] = {}
    
    @classmethod
    def register_engine(cls, engine_type: str, engine_class: Type[BaseEngineService]) -> None:
        """
        Register a new engine type
        
        Args:
            engine_type: The type name of the engine
            engine_class: The engine class to register
        """
        cls._registry[engine_type] = engine_class
        logger.info(f"Registered engine type: {engine_type}")
    
    @classmethod
    async def get_engine(cls, 
                       engine_type: str, 
                       db: AsyncSession = None, 
                       init_params: Dict[str, Any] = None) -> Optional[BaseEngineService]:
        """
        Get an engine instance by type
        
        Args:
            engine_type: The type of engine to create
            db: Database session
            init_params: Parameters for engine initialization
            
        Returns:
            Engine instance or None if engine type not found
        """
        # Use cached instance if available
        if engine_type in cls._instances:
            return cls._instances[engine_type]
            
        # Check if engine type is registered
        if engine_type not in cls._registry:
            logger.error(f"Unknown engine type: {engine_type}")
            return None
            
        # Create engine instance
        engine_class = cls._registry[engine_type]
        engine = engine_class(db)
        
        # Initialize the engine
        init_params = init_params or {}
        success = await engine.initialize(**init_params)
        if not success:
            logger.error(f"Failed to initialize engine: {engine_type}")
            return None
            
        # Cache the instance
        cls._instances[engine_type] = engine
        
        return engine
    
    @classmethod
    def get_available_engines(cls) -> List[str]:
        """
        Get list of available engine types
        
        Returns:
            List of engine type names
        """
        return list(cls._registry.keys())
    
    @classmethod
    def clear_cache(cls, engine_type: str = None) -> None:
        """
        Clear the engine instance cache
        
        Args:
            engine_type: Specific engine type to clear, or None for all
        """
        if engine_type:
            if engine_type in cls._instances:
                del cls._instances[engine_type]
                logger.info(f"Cleared cached instance of engine: {engine_type}")
        else:
            cls._instances.clear()
            logger.info("Cleared all cached engine instances") 