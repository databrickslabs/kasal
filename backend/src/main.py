import os
import sys
import logging
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from sqlalchemy import text

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from src.config.settings import settings
from src.api import api_router
from src.core.logger import LoggerManager
from src.db.session import get_db, async_session_factory
from src.services.scheduler_service import SchedulerService

# Set up basic logging initially, will be enhanced in lifespan
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Set debug flag for seeders
os.environ["SEED_DEBUG"] = "True"

# Set log directory environment variable
log_path = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), "logs")
os.environ["LOG_DIR"] = log_path
# Create logs directory if it doesn't exist
os.makedirs(log_path, exist_ok=True)

# Define paths for docs
package_dir = Path(__file__).parent.parent.parent.parent  # Go up to the package root
docs_path = package_dir / "docs" / "site"  # Path to MkDocs static site

# If we're running from the wheel package
if not docs_path.exists():
    kasal_package = None
    for path in sys.path:
        potential_path = Path(path) / "kasal" / "docs" / "site"
        if potential_path.exists():
            docs_path = potential_path
            kasal_package = Path(path) / "kasal"
            break

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager for the FastAPI application.
    
    Handles startup and shutdown events for the application.
    """
    # Initialize the centralized logging system
    log_dir = os.environ.get("LOG_DIR")
    logger_manager = LoggerManager.get_instance(log_dir)
    logger_manager.initialize()
    
    system_logger = logger_manager.system
    system_logger.info(f"Starting application... Logs will be stored in: {log_dir}")
    
    # Import needed for DB init
    # pylint: disable=unused-import,import-outside-toplevel
    from src.db.base import Base
    import src.db.all_models  # noqa
    from src.db.session import init_db
    
    # Initialize database first - this creates both the file and tables
    system_logger.info("Initializing database during lifespan...")
    try:
        await init_db()
        system_logger.info("Database initialization complete")
    except Exception as e:
        system_logger.error(f"Database initialization failed: {str(e)}")
    
    # Now check if database exists and tables are initialized
    scheduler = None
    db_initialized = False
    
    try:
        # Simple check for tables - just check if the database file exists with content
        if str(settings.DATABASE_URI).startswith('sqlite'):
            db_path = settings.SQLITE_DB_PATH
            
            # Get absolute path if relative
            if not os.path.isabs(db_path):
                db_path = os.path.abspath(db_path)
            
            system_logger.info(f"Checking database at: {db_path}")
            
            if os.path.exists(db_path) and os.path.getsize(db_path) > 0:
                # Try to execute a simple query to verify tables
                try:
                    # Direct SQLite check - more reliable than trying to use SQLAlchemy
                    import sqlite3
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
                    if cursor.fetchone():
                        system_logger.info("Database tables verified")
                        db_initialized = True
                    else:
                        system_logger.warning("Database file exists but contains no tables")
                    conn.close()
                except Exception as e:
                    system_logger.warning(f"Error checking database tables: {e}")
            else:
                system_logger.warning(f"Database file doesn't exist or is empty at: {db_path}")
        else:
            # For other database types, try a simple connection
            try:
                async with async_session_factory() as session:
                    await session.execute(text("SELECT 1"))
                    await session.commit()
                    db_initialized = True
                    system_logger.info("Database connection successful")
            except Exception as e:
                system_logger.warning(f"Database connection failed: {e}")
    except Exception as e:
        system_logger.error(f"Error checking database: {e}")
    
    # Run database seeders after DB initialization
    if db_initialized:
        # Import needed for seeders
        # pylint: disable=unused-import,import-outside-toplevel
        from src.seeds.seed_runner import run_all_seeders
        
        # Check if seeding is enabled
        should_seed = settings.AUTO_SEED_DATABASE
        system_logger.info(f"AUTO_SEED_DATABASE setting: {settings.AUTO_SEED_DATABASE}")
        
        # Run seeders if enabled
        if should_seed:
            system_logger.info("Running database seeders...")
            try:
                system_logger.info("Starting run_all_seeders() from lifespan...")
                await run_all_seeders()
                system_logger.info("Database seeding completed successfully!")
            except Exception as e:
                system_logger.error(f"Error running seeders: {str(e)}")
                import traceback
                error_trace = traceback.format_exc()
                system_logger.error(f"Seeder error trace: {error_trace}")
                # Don't raise so app can start even if seeding fails
        else:
            system_logger.info("Database seeding skipped (AUTO_SEED_DATABASE is False)")
    else:
        system_logger.warning("Skipping seeding as database is not initialized.")
    
    # Initialize scheduler on startup only if database is initialized
    if db_initialized:
        system_logger.info("Initializing scheduler...")
        try:
            # Get database connection
            db_gen = get_db()
            db = await anext(db_gen)
            
            # Initialize scheduler service
            scheduler = SchedulerService(db)
            await scheduler.start_scheduler()
            system_logger.info("Scheduler started successfully.")
        except Exception as e:
            system_logger.error(f"Failed to start scheduler: {e}")
            # Don't raise here, let the application start without scheduler
    else:
        system_logger.warning("Skipping scheduler initialization. Database not ready.")
    
    try:
        yield
    finally:
        # Shutdown scheduler if it was started
        if scheduler:
            system_logger.info("Shutting down scheduler...")
            try:
                await scheduler.shutdown()
                system_logger.info("Scheduler shut down successfully.")
            except Exception as e:
                system_logger.error(f"Error during scheduler shutdown: {e}")
        
        system_logger.info("Application shutdown complete.")

# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.VERSION,
    lifespan=lifespan,
    # Move API docs to /api-docs to free up /docs for MkDocs
    docs_url="/api-docs" if settings.DOCS_ENABLED else None,
    redoc_url="/api-redoc" if settings.DOCS_ENABLED else None,
    openapi_url="/api-openapi.json" if settings.DOCS_ENABLED else None,
    openapi_version="3.1.0"  # Explicitly set OpenAPI version
)

# Add CORS middleware with explicit allowed origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Include the main API router
app.include_router(api_router, prefix=settings.API_V1_STR)

# Mount the MkDocs static site at /docs if it exists
if docs_path.exists():
    logger.info(f"Mounting MkDocs documentation at /docs from {docs_path}")
    app.mount("/docs", StaticFiles(directory=str(docs_path), html=True), name="docs")
    
    # Override the automatic redirect to ensure it works
    @app.get("/docs", include_in_schema=False)
    async def docs_redirect():
        return RedirectResponse(url="/docs/index.html")
        
    @app.get("/", include_in_schema=False)
    async def root_redirect():
        """Redirect root to documentation"""
        return RedirectResponse(url="/docs/index.html")
else:
    logger.warning(f"MkDocs documentation path not found at {docs_path}")
    
    # If MkDocs site is not available, redirect /docs to /api-docs
    @app.get("/docs", include_in_schema=False)
    async def fallback_docs_redirect():
        return RedirectResponse(url="/api-docs")

@app.on_event("startup")
async def startup_event():
    """Run on application startup."""
    # Any additional startup tasks can go here
    logger.info("Application startup event triggered")
    
    # Log docs path status
    if docs_path.exists():
        logger.info(f"MkDocs documentation is available at /docs")
    else:
        logger.warning(f"MkDocs documentation path not found at {docs_path}")
        logger.info("API documentation is available at /api-docs")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    logger.info("Shutting down...")

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host=settings.SERVER_HOST,
        port=settings.SERVER_PORT,
        reload=settings.DEBUG_MODE,
    )
