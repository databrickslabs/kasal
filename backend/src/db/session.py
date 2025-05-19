from typing import AsyncGenerator, Generator
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
import sys

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker

from src.config.settings import settings
from src.db.base import Base
from src.core.logger import LoggerManager

# Configure logging using LoggerManager
logger_manager = LoggerManager.get_instance()
if not logger_manager._initialized or not logger_manager._log_dir:
    # Initialize with environment variable if available
    log_dir = os.environ.get("LOG_DIR")
    if log_dir:
        logger_manager.initialize(log_dir)
    else:
        logger_manager.initialize()

# Get a logger from the LoggerManager system
logger = logging.getLogger(__name__)

# Create a SQLAlchemy logger using the LoggerManager
class SQLAlchemyLogger:
    def __init__(self):
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log_dir = logger_manager._log_dir
        self.setup_logger()
    
    def setup_logger(self):
        # Create sqlalchemy.log file handler
        sqlalchemy_log_file = self.log_dir / "sqlalchemy.log"
        
        # Ensure the sqlalchemy engine logger doesn't propagate logs to stdout
        engine_logger = logging.getLogger('sqlalchemy.engine')
        engine_logger.propagate = False
        
        # Ensure handlers are set up properly
        if not engine_logger.handlers:
            # Create file handler if not already configured elsewhere
            file_handler = logging.handlers.RotatingFileHandler(
                sqlalchemy_log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setFormatter(self.formatter)
            engine_logger.addHandler(file_handler)
        
        # Log that the database logger has been configured
        logger.info(f"SQLAlchemy logs will be written to {sqlalchemy_log_file}")
        
# Initialize SQLAlchemy logging
sql_logger = SQLAlchemyLogger()

# Create async engine for the database
engine = create_async_engine(
    str(settings.DATABASE_URI),
    echo=False,  # Setting to False to disable SQL echoing to stdout
    future=True,
    pool_pre_ping=True,
)

# Create sync engine for backwards compatibility if needed
sync_engine = create_engine(
    str(settings.SYNC_DATABASE_URI),
    echo=False,
    future=True,
)

# Create async session factory
async_session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,
)

# Create sync session factory for backward compatibility
SessionLocal = sessionmaker(
    bind=sync_engine,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

# Database initialization
async def init_db() -> None:
    """Initialize database tables if they don't exist."""
    try:
        # Import all models to ensure they're registered
        import importlib
        import src.db.all_models
        importlib.reload(src.db.all_models)  # Ensure models are freshly loaded
        from src.db.all_models import Base
        
        # For SQLite, ensure database file exists
        if str(settings.DATABASE_URI).startswith('sqlite'):
            db_path = settings.SQLITE_DB_PATH
            
            # Get absolute path if relative
            if not os.path.isabs(db_path):
                # If it's a relative path, make it absolute from current directory
                db_path = os.path.abspath(db_path)
            
            logger.info(f"Database path: {db_path}")
            
            # Create directory if it doesn't exist
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                logger.info(f"Creating database directory: {db_dir}")
                os.makedirs(db_dir, exist_ok=True)
            
            # Create empty database file if it doesn't exist
            if not os.path.exists(db_path):
                logger.info(f"Creating new SQLite database file: {db_path}")
                # Create the file and initialize it
                with open(db_path, 'w') as f:
                    pass  # Create empty file
                
                # Initialize it as a sqlite database
                import sqlite3
                conn = sqlite3.connect(db_path)
                conn.close()
                logger.info(f"Empty database file created at {db_path}")
        
        # Create all tables in a completely separate, isolated transaction
        logger.info("Creating database tables...")
        
        # For SQLite, we can verify if tables already exist first
        tables_exist = False
        if str(settings.DATABASE_URI).startswith('sqlite'):
            try:
                import sqlite3
                conn = sqlite3.connect(os.path.abspath(settings.SQLITE_DB_PATH))
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                conn.close()
                
                if len(tables) > 1:  # SQLite has a sqlite_master table by default
                    logger.info(f"Tables already exist: {', '.join([t[0] for t in tables])}")
                    tables_exist = True
            except Exception as e:
                logger.error(f"Error checking existing tables: {e}")
        
        # Only create tables if they don't already exist
        if not tables_exist:
            # Use a fresh engine for initialization with settings optimized for table creation
            init_engine_opts = {
                "isolation_level": "AUTOCOMMIT",  # Use AUTOCOMMIT for table creation
                "echo": True,  # Enable detailed logging for debugging
                "future": True,
            }
            
            # Create a dedicated engine just for initialization
            engine_for_init = create_async_engine(
                str(settings.DATABASE_URI),
                **init_engine_opts
            )
            
            # First ensure connection works
            async with engine_for_init.connect() as conn:
                logger.info("Database connection established")
            
            # Then create tables
            try:
                async with engine_for_init.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                    logger.info("Tables created successfully")
            except Exception as table_error:
                logger.error(f"Error creating tables: {table_error}")
                import traceback
                logger.error(traceback.format_exc())
                raise
            
            # Close the engine after use
            await engine_for_init.dispose()
            
            logger.info("Database tables initialized successfully")
        
        # Verify tables were created for SQLite
        if str(settings.DATABASE_URI).startswith('sqlite'):
            import sqlite3
            try:
                db_path_to_check = os.path.abspath(settings.SQLITE_DB_PATH) 
                logger.info(f"Verifying tables in: {db_path_to_check}")
                
                if os.path.exists(db_path_to_check) and os.path.getsize(db_path_to_check) > 0:
                    conn = sqlite3.connect(db_path_to_check)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = cursor.fetchall()
                    conn.close()
                    
                    table_count = len(tables)
                    logger.info(f"Verified {table_count} tables in database: {', '.join([t[0] for t in tables])}")
                    if table_count == 0:
                        logger.error("No tables were created in the database!")
                else:
                    logger.error(f"Database file not found or empty after initialization: {db_path_to_check}")
            except Exception as e:
                logger.error(f"Error verifying tables: {e}")
                import traceback
                logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        # Print full traceback for debugging
        import traceback
        logger.error(traceback.format_exc())
        raise

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function that yields db sessions.
    
    Yields:
        AsyncSession: SQLAlchemy async session
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

# Keep the sync version for backward compatibility
def get_sync_db() -> Generator[SessionLocal, None, None]:
    """Synchronous database session for backward compatibility"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 