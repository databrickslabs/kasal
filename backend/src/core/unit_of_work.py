"""
Unit of Work Pattern Implementation

This module implements the Unit of Work pattern for managing
database transactions and repository lifecycle.
"""

import logging
from typing import Optional

from src.repositories.tool_repository import ToolRepository
from src.repositories.api_key_repository import ApiKeyRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.repositories.template_repository import TemplateRepository
from src.repositories.task_tracking_repository import TaskTrackingRepository
from src.repositories.schema_repository import SchemaRepository
from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.repositories.mcp_repository import MCPServerRepository, MCPSettingsRepository
from src.repositories.engine_config_repository import EngineConfigRepository

logger = logging.getLogger(__name__)

class UnitOfWork:
    """
    Manages repositories and transactions as a unit.
    
    This class implements the Unit of Work pattern to ensure that all
    database operations within a transaction are atomic and consistent.
    All repositories share the same session within a single unit of work.
    """
    
    def __init__(self):
        self._session = None
        self.tool_repository: Optional[ToolRepository] = None
        self.api_key_repository: Optional[ApiKeyRepository] = None
        self.model_config_repository: Optional[ModelConfigRepository] = None
        self.template_repository: Optional[TemplateRepository] = None
        self.task_tracking_repository: Optional[TaskTrackingRepository] = None
        self.schema_repository: Optional[SchemaRepository] = None
        self.databricks_config_repository: Optional[DatabricksConfigRepository] = None
        self.mcp_server_repository: Optional[MCPServerRepository] = None
        self.mcp_settings_repository: Optional[MCPSettingsRepository] = None
        self.engine_config_repository: Optional[EngineConfigRepository] = None
    
    async def __aenter__(self):
        """
        Enter async context and create all repositories with a single session.
        
        Returns:
            UnitOfWork: Self reference with all repositories initialized
        """
        from src.db.session import async_session_factory
        self._session = async_session_factory()
        session = await self._session.__aenter__()
        
        # Create repositories with the shared session
        self.tool_repository = ToolRepository(session)
        self.api_key_repository = ApiKeyRepository(session)
        self.model_config_repository = ModelConfigRepository(session)
        self.template_repository = TemplateRepository(session)
        self.task_tracking_repository = TaskTrackingRepository(session)
        self.schema_repository = SchemaRepository(session)
        self.databricks_config_repository = DatabricksConfigRepository(session)
        self.mcp_server_repository = MCPServerRepository(session)
        self.mcp_settings_repository = MCPSettingsRepository(session)
        self.engine_config_repository = EngineConfigRepository(session)
        
        logger.debug("UnitOfWork initialized with repositories")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the async context, committing or rolling back as appropriate.
        
        Args:
            exc_type: Exception type if an exception occurred, else None
            exc_val: Exception value if an exception occurred, else None
            exc_tb: Exception traceback if an exception occurred, else None
        """
        try:
            # If there was an exception, rollback
            if exc_type is not None:
                logger.debug(f"UnitOfWork exiting with exception, rolling back: {exc_type.__name__}: {exc_val}")
                await self._session.rollback()
            else:
                # If no exception, commit any pending changes that weren't explicitly committed
                try:
                    await self._session.commit()
                    logger.debug("UnitOfWork context committed on exit")
                except Exception as commit_error:
                    logger.error(f"Error committing in UnitOfWork.__aexit__: {commit_error}")
                    await self._session.rollback()
                    raise
        finally:
            # Always close the session to release connections back to the pool
            await self._session.close()
            logger.debug("UnitOfWork session closed and released to pool")
            
            # Additional cleanup to help the garbage collector
            self.tool_repository = None
            self.api_key_repository = None
            self.model_config_repository = None
            self.template_repository = None
            self.task_tracking_repository = None
            self.schema_repository = None
            self.databricks_config_repository = None
            self.mcp_server_repository = None
            self.mcp_settings_repository = None
            self.engine_config_repository = None
    
    async def commit(self):
        """
        Explicitly commit the current transaction.
        """
        try:
            await self._session.commit()
            logger.debug("UnitOfWork transaction committed")
        except Exception as e:
            logger.error(f"Error committing UnitOfWork transaction: {str(e)}")
            raise

class SyncUnitOfWork:
    """
    Synchronous version of UnitOfWork for non-async contexts.
    
    This class implements the Unit of Work pattern for synchronous operations
    used in callbacks and other non-async contexts.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """
        Get or create a singleton instance of SyncUnitOfWork.
        
        Returns:
            SyncUnitOfWork: Singleton instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize the unit of work with repository references."""
        self._session = None
        self.tool_repository = None
        self.api_key_repository = None
        self.model_config_repository = None
        self.template_repository = None
        self.task_tracking_repository = None
        self.schema_repository = None
        self.databricks_config_repository = None
        self.mcp_server_repository = None
        self.mcp_settings_repository = None
        self.engine_config_repository = None
        self._initialized = False
    
    def initialize(self):
        """Initialize repositories with a session."""
        if not self._initialized:
            from src.db.session import SessionLocal
            self._session = SessionLocal()
            
            # Initialize repositories with the sync session
            self.tool_repository = ToolRepository(self._session)
            self.api_key_repository = ApiKeyRepository(self._session)
            self.model_config_repository = ModelConfigRepository(self._session)
            self.template_repository = TemplateRepository(self._session)
            self.task_tracking_repository = TaskTrackingRepository(self._session)
            self.schema_repository = SchemaRepository(self._session)
            self.databricks_config_repository = DatabricksConfigRepository(self._session)
            self.mcp_server_repository = MCPServerRepository(self._session)
            self.mcp_settings_repository = MCPSettingsRepository(self._session)
            self.engine_config_repository = EngineConfigRepository(self._session)
            
            self._initialized = True
            logger.debug("SyncUnitOfWork initialized with repositories")
    
    def commit(self):
        """Commit the current transaction."""
        if not self._initialized:
            raise RuntimeError("SyncUnitOfWork not initialized")
        
        try:
            self._session.commit()
            logger.debug("SyncUnitOfWork transaction committed")
        except Exception as e:
            self._session.rollback()
            logger.error(f"Error committing SyncUnitOfWork transaction: {str(e)}")
            raise
    
    def rollback(self):
        """Rollback the current transaction."""
        if not self._initialized:
            raise RuntimeError("SyncUnitOfWork not initialized")
        
        try:
            self._session.rollback()
            logger.debug("SyncUnitOfWork transaction rolled back")
        except Exception as e:
            logger.error(f"Error rolling back SyncUnitOfWork transaction: {str(e)}")
            raise
    
    def cleanup(self):
        """Clean up resources."""
        if self._initialized and self._session is not None:
            self._session.close()
            self._session = None
            self._initialized = False
            logger.debug("SyncUnitOfWork resources cleaned up")
            
    def __del__(self):
        """Ensure resources are cleaned up on deletion."""
        self.cleanup() 