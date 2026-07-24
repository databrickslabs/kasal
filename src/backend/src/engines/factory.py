"""
Engine factory module.

This module provides factory methods for creating and accessing
engine service instances.
"""

import logging
from typing import Dict, Type, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio

from src.engines.base.base_engine_service import BaseEngineService
from src.engines.kasal.kasal_engine_service import KasalEngineService

logger = logging.getLogger(__name__)

class EngineFactory:
    """Factory for creating engine instances."""
    
    @staticmethod
    async def get_engine(
        engine_type: str,
        initialize: bool = True,
        llm_provider: str = None,
        model: str = None
    ) -> Optional[Any]:
        """
        Get an engine instance.
        
        Args:
            engine_type: Type of engine to create
            initialize: Whether to initialize the engine
            llm_provider: LLM provider to use
            model: Model to use
            
        Returns:
            Engine instance or None if not found
        """
        try:
            # "crewai" is the legacy name for the kasal engine (old DB rows/payloads)
            if engine_type in ("kasal", "crewai"):
                from src.engines.kasal.kasal_engine_service import KasalEngineService
                engine = KasalEngineService()
                if initialize:
                    # Create initialization task but don't await it
                    init_task = asyncio.create_task(engine.initialize(llm_provider=llm_provider, model=model))
                    # Store the task on the engine instance for later reference
                    engine._init_task = init_task
                return engine
            else:
                raise ValueError(f"Unknown engine type: {engine_type}")
        except Exception as e:
            logger.error(f"Error creating engine: {str(e)}")
            return None
    
    @classmethod
    def register_engine(cls, engine_type: str, engine_class: Type[BaseEngineService]) -> None:
        """
        Register a new engine type with the factory.
        
        Args:
            engine_type: Type of engine (e.g., "kasal")
            engine_class: Class implementing BaseEngineService
        """
        # This method is no longer used in the new implementation
        pass 