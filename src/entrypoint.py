#!/usr/bin/env python3
"""
Kasal entrypoint module.

This module serves as the entrypoint for the Kasal application when running from source.
It starts the FastAPI backend server and serves the frontend static files.
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import asyncio
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("kasal")

def create_parser():
    """Create argument parser for CLI options."""
    parser = argparse.ArgumentParser(description="Kasal application")
    parser.add_argument(
        "--db-type",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help="Database type to use (sqlite or postgres)"
    )
    parser.add_argument(
        "--db-url",
        help="Database URL (for postgres: postgresql://user:password@host:port/dbname)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to run the server on"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development"
    )
    return parser

class SPAMiddleware(BaseHTTPMiddleware):
    """Middleware to serve SPA frontend for non-API routes."""
    
    def __init__(self, app, frontend_dir):
        super().__init__(app)
        # Convert to Path object if it's a string
        self.frontend_dir = Path(frontend_dir) if isinstance(frontend_dir, str) else frontend_dir
        self.index_path = self.frontend_dir / "index.html"
        
        # Verify that index.html exists
        if not self.index_path.exists():
            logger.error(f"index.html not found at {self.index_path}")
        else:
            logger.info(f"Found index.html at {self.index_path}")
            
            # Log frontend directory contents
            logger.info(f"Frontend directory contents:")
            for item in os.listdir(str(self.frontend_dir)):
                logger.info(f"  - {item}")
    
    async def dispatch(self, request, call_next):
        path = request.url.path
        
        # Skip API routes, static files, and markdown files
        if (path.startswith("/api/") or 
            path.startswith("/api-docs") or 
            path.startswith("/static/") or 
            path == "/favicon.ico" or
            path.endswith(".md")):
            # Let the main router handle these paths
            logger.info(f"Skipping middleware for path: {path}")
            return await call_next(request)
        
        # For all other routes, serve the SPA's index.html
        if self.index_path.exists():
            logger.info(f"Serving SPA for path: {path}")
            with open(self.index_path, "r") as f:
                content = f.read()
            return HTMLResponse(content)
        
        # If index.html doesn't exist, continue with normal routing
        return await call_next(request)

async def initialize_database():
    """
    Initialize database - directly implemented from main.py lifespan.
    """
    logger.info("Initializing database...")
    
    try:
        # Import modules directly
        from backend.src.config.settings import settings
        from backend.src.db.session import init_db, async_session_factory, get_db
        from backend.src.core.logger import LoggerManager
        
        # Get the logger
        log_dir = os.environ.get("LOG_DIR")
        logger_manager = LoggerManager.get_instance(log_dir)
        if not logger_manager._initialized:
            logger_manager.initialize()
        
        system_logger = logger_manager.system
        
        # Initialize database first
        system_logger.info("Initializing database...")
        try:
            await init_db()
            system_logger.info("Database initialization complete")
        except Exception as e:
            system_logger.error(f"Database initialization failed: {str(e)}")
            raise
        
        # Now check if database exists and tables are initialized
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
            try:
                from backend.src.seeds.seed_runner import run_all_seeders
                
                # Check if seeding is enabled
                should_seed = settings.AUTO_SEED_DATABASE
                system_logger.info(f"AUTO_SEED_DATABASE setting: {settings.AUTO_SEED_DATABASE}")
                
                # Run seeders if enabled
                if should_seed:
                    system_logger.info("Running database seeders...")
                    try:
                        system_logger.info("Starting run_all_seeders()...")
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
            except Exception as e:
                system_logger.error(f"Error loading seed_runner module: {e}")
        else:
            system_logger.warning("Skipping seeding as database is not initialized.")
        
        return db_initialized
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

def run_app():
    """Run the Kasal application."""
    # Parse command line arguments
    parser = create_parser()
    args = parser.parse_args()
    
    logger.info("Starting Kasal application")
    
    # Get project root directory
    project_root = Path(__file__).parent
    logger.info(f"Project root directory: {project_root}")
    
    # Set environment variables for the frontend static files
    frontend_static_dir = str(project_root / "frontend_static")
    os.environ["FRONTEND_STATIC_DIR"] = frontend_static_dir
    logger.info(f"Frontend static directory: {frontend_static_dir}")

    # Create logs directory
    logs_dir = project_root / "backend" / "logs"
    os.makedirs(str(logs_dir), exist_ok=True)
    os.environ["LOG_DIR"] = str(logs_dir)
    logger.info(f"Created logs directory at: {logs_dir}")

    # Set database configuration
    if args.db_type == "postgres":
        # Use PostgreSQL
        if args.db_url:
            db_url = args.db_url
        else:
            logger.warning("No database URL provided for PostgreSQL. Using default.")
            db_url = "postgresql://postgres:postgres@localhost:5432/kasal"
        
        os.environ["DATABASE_URL"] = db_url
        os.environ["DATABASE_URI"] = db_url  # Set both variables
    else:
        # Use SQLite (default)
        db_path = os.environ.get("SQLITE_DB_PATH", str(project_root / "kasal.db"))
        
        # Set all required environment variables
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
        os.environ["DATABASE_URI"] = f"sqlite+aiosqlite:///{db_path}"  # Use aiosqlite for async operations
        os.environ["SQLITE_DB_PATH"] = db_path  # Explicitly set the SQLITE_DB_PATH
        
        logger.info(f"Using SQLite database at: {db_path}")
    
    # Add the backend directory to Python path to handle src imports
    backend_dir = project_root / "backend"
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    
    try:
        # Import the main module directly
        from src.main import app as original_app
        logger.info("Found FastAPI app in main module")
        
        # Create a new FastAPI app with the SAME lifespan handler
        app = FastAPI(
            title=original_app.title, 
            description=original_app.description,
            version=original_app.version,
            lifespan=getattr(original_app, 'lifespan', None),  # Copy the lifespan from the original app
            # Move API docs to /api-docs
            docs_url="/api-docs",
            redoc_url="/api-redoc",
            openapi_url="/api-openapi.json"
        )
        
        # Copy all routes from the original app to our new app
        for route in original_app.routes:
            app.routes.append(route)
        
        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        
        # Mount static files if they exist
        if os.path.exists(frontend_static_dir):
            # Check if there's a static directory inside frontend_static_dir
            static_dir = os.path.join(frontend_static_dir, "static")
            if os.path.exists(static_dir):
                logger.info(f"Mounting /static from {static_dir}")
                app.mount("/static", StaticFiles(directory=static_dir), name="static")
            else:
                # Maybe the static files are directly in the frontend_static_dir
                logger.info(f"Mounting /static directly from {frontend_static_dir}")
                app.mount("/static", StaticFiles(directory=frontend_static_dir), name="static")
            
            # Mount individual static assets
            favicon_ico = os.path.join(frontend_static_dir, "favicon.ico")
            if os.path.exists(favicon_ico):
                @app.get("/favicon.ico")
                async def serve_favicon_ico():
                    return FileResponse(favicon_ico)
            
            manifest_json = os.path.join(frontend_static_dir, "manifest.json")
            if os.path.exists(manifest_json):
                @app.get("/manifest.json")
                async def serve_manifest():
                    return FileResponse(manifest_json)
            
            # Serve markdown files from docs directory
            docs_dir = os.path.join(frontend_static_dir, "docs")
            if os.path.exists(docs_dir):
                @app.get("/docs/{filename}")
                async def serve_docs(filename: str):
                    if filename.endswith('.md'):
                        file_path = os.path.join(docs_dir, filename)
                        if os.path.exists(file_path):
                            return FileResponse(file_path, media_type="text/markdown")
                    raise HTTPException(status_code=404, detail="File not found")
        
        # Add middleware to serve frontend for all non-API routes
        app.add_middleware(SPAMiddleware, frontend_dir=frontend_static_dir)
        
        # Initialize the database
        try:
            # Create a new event loop 
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            logger.info("Running database initialization...")
            db_initialized = loop.run_until_complete(initialize_database())
            logger.info(f"Database initialization complete (success: {db_initialized})")
        except Exception as e:
            logger.error(f"Error during database initialization: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Import uvicorn to run the app
        import uvicorn
        
        # Run the app with uvicorn
        logger.info(f"Starting server on port {args.port}")
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=args.port,
            reload=args.reload,
            log_level="info"
        )
    except Exception as e:
        logger.error(f"Error starting Kasal application: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    run_app() 