#!/usr/bin/env python3
"""
Kasal entrypoint module.

This module serves as the entrypoint for the Kasal application when installed
as a wheel package. It starts the FastAPI backend server and serves the
frontend static files.
"""

import os
import sys
import logging
import importlib.util
import argparse
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
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

def find_package_path():
    """Find the installed package path."""
    try:
        # Try to find the package path
        spec = importlib.util.find_spec("kasal")
        if spec is None:
            return None
        return Path(spec.origin).parent
    except (ImportError, AttributeError):
        return None

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
        
        # Skip API routes, documentation, and static files
        if (path.startswith("/api/") or 
            path.startswith("/api-docs") or 
            path == "/docs" or 
            path.startswith("/docs/") or 
            path.startswith("/static/") or 
            path == "/favicon.ico"):
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

async def initialize_database(package_path):
    """
    Initialize database - directly implemented from main.py lifespan.
    """
    logger.info("Initializing database...")
    
    # Import modules directly using importlib
    try:
        # Load settings module
        settings_path = package_path / "backend" / "src" / "config" / "settings.py"
        spec = importlib.util.spec_from_file_location("settings", settings_path)
        settings_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(settings_module)
        settings = settings_module.settings
        
        # Load session module
        session_path = package_path / "backend" / "src" / "db" / "session.py"
        spec = importlib.util.spec_from_file_location("session", session_path)
        session_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(session_module)
        init_db = session_module.init_db
        async_session_factory = session_module.async_session_factory
        get_db = session_module.get_db
        
        # Load logger manager
        logger_path = package_path / "backend" / "src" / "core" / "logger.py"
        spec = importlib.util.spec_from_file_location("logger", logger_path)
        logger_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(logger_module)
        LoggerManager = logger_module.LoggerManager
        
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
            # Import needed for seeders
            try:
                # Load seed_runner module
                seed_runner_path = package_path / "backend" / "src" / "seeds" / "seed_runner.py"
                spec = importlib.util.spec_from_file_location("seed_runner", seed_runner_path)
                seed_runner_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(seed_runner_module)
                run_all_seeders = seed_runner_module.run_all_seeders
                
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
    
    # Find the package path
    package_path = find_package_path()
    if package_path is None:
        logger.error("Could not find the Kasal package. Is it installed?")
        sys.exit(1)
    
    # Set environment variables for the frontend static files
    frontend_static_dir = str(package_path / "frontend" / "static")
    os.environ["FRONTEND_STATIC_DIR"] = frontend_static_dir
    logger.info(f"Frontend static directory: {frontend_static_dir}")

    # Create logs directory inside backend, not inside backend/src
    logs_dir = package_path / "backend" / "logs"
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
        db_path = os.environ.get("SQLITE_DB_PATH", str(Path.home() / "kasal.db"))
        
        # Set all required environment variables
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
        os.environ["DATABASE_URI"] = f"sqlite+aiosqlite:///{db_path}"  # Use aiosqlite for async operations
        os.environ["SQLITE_DB_PATH"] = db_path  # Explicitly set the SQLITE_DB_PATH
        
        logger.info(f"Using SQLite database at: {db_path}")
    
    # Import and run the backend server
    backend_main = package_path / "backend" / "src" / "main.py"
    if not backend_main.exists():
        logger.error(f"Backend main module not found at {backend_main}")
        sys.exit(1)
    
    try:
        # Add the parent directory of src to the Python path
        # This allows imports like 'src.dependencies' to work
        backend_src_parent = str(package_path / "backend")
        if backend_src_parent not in sys.path:
            sys.path.insert(0, backend_src_parent)
        
        # Also set up an alias for the backend src directory
        # This helps with imports expecting 'src' to be at the root
        src_path = str(package_path / "backend" / "src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        # Create a symlink for backwards compatibility with old imports
        # This is a workaround for code that uses absolute imports
        current_dir = os.getcwd()
        if not os.path.exists(os.path.join(current_dir, "src")):
            try:
                # Create a symlink if possible
                if hasattr(os, 'symlink'):
                    os.symlink(src_path, os.path.join(current_dir, "src"))
                    logger.info(f"Created symlink for src directory")
            except Exception as e:
                logger.warning(f"Could not create symlink for src directory: {e}")
        
        # Dynamically import the main module
        spec = importlib.util.spec_from_file_location("main", backend_main)
        main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main)
        
        # Initialize the database directly
        try:
            # Create a new event loop 
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            logger.info("Running database initialization...")
            db_initialized = loop.run_until_complete(initialize_database(package_path))
            logger.info(f"Database initialization complete (success: {db_initialized})")
        except Exception as e:
            logger.error(f"Error during database initialization: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Create our own FastAPI app that wraps the main app 
        # with middleware to serve the frontend
        if hasattr(main, "app"):
            # Get the original app
            original_app = main.app
            logger.info("Found FastAPI app in main module")
            
            # Create a new FastAPI app with the SAME lifespan handler
            app = FastAPI(
                title=original_app.title, 
                description=original_app.description,
                version=original_app.version,
                lifespan=getattr(original_app, 'lifespan', None),  # Copy the lifespan from the original app
                # Move API docs to /api-docs to free up /docs for MkDocs
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
            
            # Mount MkDocs documentation if it exists
            docs_site_dir = package_path / "docs" / "site"
            if docs_site_dir.exists():
                # Use existing docs site directory
                logger.info(f"Mounting MkDocs documentation at /docs from {docs_site_dir}")
                # Log the contents of the docs site directory
                logger.info(f"Contents of docs site directory:")
                for item in os.listdir(str(docs_site_dir)):
                    logger.info(f"  - {item}")
                
                # Check that index.html exists
                index_html = docs_site_dir / "index.html"
                if not index_html.exists():
                    logger.warning(f"index.html not found in {docs_site_dir}, documentation may not display correctly")
                
                # Mount the static files
                app.mount("/docs", StaticFiles(directory=str(docs_site_dir), html=True), name="docs")
                
                # Create a direct route to the index.html file rather than relying on redirects
                @app.get("/docs", include_in_schema=False)
                async def docs_index():
                    logger.info("Directly serving docs index.html")
                    with open(index_html, "r") as f:
                        content = f.read()
                    return HTMLResponse(content=content, media_type="text/html")
                
                # Also redirect root to docs
                @app.get("/", include_in_schema=False)
                async def root_to_docs():
                    logger.info("Redirecting root to /docs")
                    return RedirectResponse(url="/docs")
                
            else:
                # Look in alternative locations
                alternative_paths = [
                    package_path.parent / "docs" / "site",  # Check one level up
                    Path(os.getcwd()) / "site",  # Current directory
                ]
                
                # Also look in directories inside site-packages
                try:
                    import site
                    for site_dir in site.getsitepackages():
                        site_path = Path(site_dir)
                        # Look for kasal wheel or any installed docs
                        for pkg_path in site_path.glob("*kasal*"):
                            if pkg_path.is_dir():
                                docs_dir = pkg_path / "docs" / "site"
                                if docs_dir.exists():
                                    alternative_paths.append(docs_dir)
                except ImportError:
                    logger.warning("Could not import site module, skipping site-packages check")
                
                # Look in all the alternative paths
                for alt_path in alternative_paths:
                    if alt_path.exists():
                        logger.info(f"Found MkDocs documentation at alternative path: {alt_path}")
                        
                        # Log the contents of the alternative docs site directory
                        logger.info(f"Contents of alternative docs site directory:")
                        for item in os.listdir(str(alt_path)):
                            logger.info(f"  - {item}")
                        
                        # Check that index.html exists
                        alt_index_html = alt_path / "index.html"
                        if not alt_index_html.exists():
                            logger.warning(f"index.html not found in {alt_path}, documentation may not display correctly")
                        
                        # Mount the static files
                        app.mount("/docs", StaticFiles(directory=str(alt_path), html=True), name="docs")
                        
                        # Create a direct route to the index.html file rather than relying on redirects
                        @app.get("/docs", include_in_schema=False)
                        async def docs_index_alt():
                            logger.info("Directly serving docs index.html from alternative path")
                            with open(alt_index_html, "r") as f:
                                content = f.read()
                            return HTMLResponse(content=content, media_type="text/html")
                        
                        # Also redirect root to docs
                        @app.get("/", include_in_schema=False)
                        async def root_to_docs_alt():
                            logger.info("Redirecting root to /docs")
                            return RedirectResponse(url="/docs")
                        
                        break
                else:
                    logger.warning(f"MkDocs documentation not found at any expected location")
            
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
            
            # Add middleware to serve frontend for all non-API routes
            app.add_middleware(SPAMiddleware, frontend_dir=frontend_static_dir)
            
            # Define a standalone function that uvicorn can import
            def get_application():
                return app
            
            # Write the function to a temporary file that uvicorn can import
            temp_dir = Path.home() / ".kasal"
            temp_dir.mkdir(exist_ok=True)
            temp_file = temp_dir / "app.py"
            
            with open(temp_file, "w") as f:
                f.write("""
from fastapi import FastAPI

# This app is created dynamically by the kasal entrypoint
app = None

def set_app(application):
    global app
    app = application
""")
            
            # Import the module and set the app
            sys.path.insert(0, str(temp_dir))
            import app as temp_app
            temp_app.set_app(app)
            
            # Import uvicorn to run the app
            import uvicorn
            
            # Run the app with uvicorn
            logger.info(f"Starting server on port {args.port}")
            uvicorn.run(
                "app:app", 
                host="0.0.0.0", 
                port=args.port,
                reload=args.reload,
                log_level="info"
            )
        else:
            logger.error("Backend main module does not have an 'app' attribute")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error starting Kasal application: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    run_app() 