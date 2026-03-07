import os
import sys

# CRITICAL: Set USE_NULLPOOL BEFORE any database imports to prevent asyncpg connection pool issues
# This must be done before importing any modules that might create database connections
os.environ["USE_NULLPOOL"] = "true"

# Configure logging BEFORE any other imports to ensure all loggers respect configuration
from src.config.logging import CentralizedLoggingConfig, configure_early_logging

configure_early_logging()

import asyncio

# Now import everything else
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from src.api import api_router
from src.config.settings import settings
from src.core.logger import LoggerManager
from src.db.session import async_session_factory, get_db
from src.services.execution_cleanup_service import ExecutionCleanupService
from src.services.scheduler_service import SchedulerService
from src.utils.databricks_url_utils import DatabricksURLUtils

# Get logger after configuration
logger = logging.getLogger(__name__)

# Print logging configuration if in debug mode
if (
    os.environ.get("KASAL_DEBUG_ALL", "").lower() in ["true", "1", "yes"]
    or os.environ.get("KASAL_LOG_LEVEL", "").upper() == "DEBUG"
):
    print(CentralizedLoggingConfig.get_configuration_summary())

# Set debug flag for seeders
os.environ["SEED_DEBUG"] = "True"

# Disable CrewAI telemetry (do NOT set OTEL_SDK_DISABLED as it disables all OTel including App Telemetry logs)
os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"

# Prevent MLflow from creating local mlruns directories
# This MUST be set before MLflow is imported anywhere to ensure Databricks backend is used
os.environ["MLFLOW_TRACKING_URI"] = "databricks"

# Set log directory environment variable
log_path = os.path.join(
    os.path.abspath(os.path.dirname(os.path.dirname(__file__))), "logs"
)
os.environ["LOG_DIR"] = log_path
# Create logs directory if it doesn't exist
os.makedirs(log_path, exist_ok=True)


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

    # Reduce noisy module logs to keep stdout manageable
    try:
        import logging as _logging

        _logging.getLogger("src.utils.user_context").setLevel(_logging.WARNING)
        _logging.getLogger("src.core.dependencies").setLevel(_logging.WARNING)
        _logging.getLogger("src.services.user_service").setLevel(_logging.WARNING)
    except Exception as _e:
        system_logger.warning(f"Failed to adjust module log levels: {_e}")

    # Validate and fix Databricks environment variables early in startup
    try:
        system_logger.info("Validating Databricks environment configuration...")
        await DatabricksURLUtils.validate_and_fix_environment()
    except Exception as e:
        system_logger.warning(f"Error validating Databricks environment: {e}")

    # Import needed for DB init
    # pylint: disable=unused-import,import-outside-toplevel
    import src.db.all_models  # noqa
    from src.db.session import init_db, set_main_event_loop

    # Capture the main event loop for smart engine selection
    set_main_event_loop()

    # Initialize database first - this creates both the file and tables
    system_logger.info("Initializing database during lifespan...")
    try:
        await init_db()
        system_logger.info("Database initialization complete")
    except Exception as e:
        system_logger.error(f"Database initialization failed: {str(e)}")

    # Start embedding queue service for SQLite to batch operations (non-blocking)
    embedding_queue_started = False
    if str(settings.DATABASE_URI).startswith("sqlite"):
        try:
            # Start the queue service in the background without blocking
            import asyncio

            from src.services.embedding_queue_service import embedding_queue

            asyncio.create_task(embedding_queue.start())
            embedding_queue_started = True
            system_logger.info(
                "Embedding queue service started in background for SQLite batch processing"
            )
        except Exception as e:
            system_logger.error(f"Failed to start embedding queue service: {e}")

    # Now check if database exists and tables are initialized
    scheduler = None
    db_initialized = False

    try:
        # Simple check for tables - just check if the database file exists with content
        if str(settings.DATABASE_URI).startswith("sqlite"):
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
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;"
                    )
                    if cursor.fetchone():
                        system_logger.info("Database tables verified")
                        db_initialized = True
                    else:
                        system_logger.warning(
                            "Database file exists but contains no tables"
                        )
                    conn.close()
                except Exception as e:
                    system_logger.warning(f"Error checking database tables: {e}")
            else:
                system_logger.warning(
                    f"Database file doesn't exist or is empty at: {db_path}"
                )
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

    # Clean up stale jobs from previous run
    if db_initialized:
        system_logger.info("Cleaning up stale jobs from previous run...")
        try:
            cleaned_jobs = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()
            if cleaned_jobs > 0:
                system_logger.info(f"Successfully cleaned up {cleaned_jobs} stale jobs")
        except Exception as e:
            system_logger.error(f"Error cleaning up stale jobs: {e}")
            # Don't raise - allow app to start even if cleanup fails

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
                # Always run seeders in background to avoid blocking startup
                import asyncio

                system_logger.info("Starting seeders in background...")

                async def run_seeders_background():
                    try:
                        system_logger.info("Background seeders started...")
                        await run_all_seeders()
                        system_logger.info(
                            "Background database seeding completed successfully!"
                        )
                    except Exception as e:
                        system_logger.error(
                            f"Error running background seeders: {str(e)}"
                        )
                        import traceback

                        error_trace = traceback.format_exc()
                        system_logger.error(
                            f"Background seeder error trace: {error_trace}"
                        )

                # Create background task
                asyncio.create_task(run_seeders_background())
                system_logger.info(
                    "Seeders started in background, application startup continues..."
                )
            except Exception as e:
                system_logger.error(f"Error starting seeders: {str(e)}")
                import traceback

                error_trace = traceback.format_exc()
                system_logger.error(f"Seeder startup error trace: {error_trace}")
                # Don't raise so app can start even if seeding fails
        else:
            system_logger.info("Database seeding skipped (AUTO_SEED_DATABASE is False)")
    else:
        system_logger.warning("Skipping seeding as database is not initialized.")

    # ── Activate OTel App Telemetry if enabled in EngineConfig (system-level) ──
    if db_initialized:
        try:
            async with async_session_factory() as session:
                from src.repositories.engine_config_repository import EngineConfigRepository
                repo = EngineConfigRepository(session)
                if await repo.get_otel_app_telemetry_enabled():
                    logger_manager.enable_otel_app_telemetry(enabled=True)
        except Exception as e:
            system_logger.warning(f"OTel App Telemetry activation skipped: {e}")

    # ── Activate Lakebase session factory if Lakebase is the configured DB ──
    # This swaps the global async_session_factory so that ALL existing callers
    # (background tasks, services, UnitOfWork) automatically use Lakebase.
    if db_initialized:
        try:
            from src.db.database_router import is_lakebase_enabled, get_lakebase_config_from_db
            if await is_lakebase_enabled():
                config = await get_lakebase_config_from_db()
                instance_name = (
                    (config or {}).get("instance_name")
                    or os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
                )
                from src.db.lakebase_session import LakebaseSessionFactory
                lb_factory = LakebaseSessionFactory(instance_name)
                await lb_factory.create_engine()
                async_session_factory.activate_lakebase(lb_factory._session_factory)
                from src.db.lakebase_state import mark_lakebase_activated
                mark_lakebase_activated()
                system_logger.info(
                    f"Activated Lakebase session factory (instance: {instance_name})"
                )
        except Exception as e:
            system_logger.warning(f"Lakebase activation skipped: {e}")

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

    # Start HITL timeout service for processing expired approvals
    hitl_timeout_started = False
    if db_initialized:
        try:
            from src.services.hitl_timeout_service import start_hitl_timeout_service

            await start_hitl_timeout_service()
            hitl_timeout_started = True
            system_logger.info("HITL timeout service started successfully")
        except Exception as e:
            system_logger.error(f"Failed to start HITL timeout service: {e}")
            # Don't raise - allow app to start without HITL timeout service

    # Start trace broadcast service for real-time SSE updates
    # This polls the database for new traces and broadcasts them to SSE clients
    # Required because subprocess executions can't broadcast SSE directly
    trace_broadcast_started = False
    try:
        from src.services.trace_broadcast_service import trace_broadcast_service

        trace_broadcast_service.start()
        trace_broadcast_started = True
        system_logger.info("Trace broadcast service started successfully")
    except Exception as e:
        system_logger.error(f"Failed to start trace broadcast service: {e}")
        # Don't raise - allow app to start without trace broadcast service

    # Start execution broadcast service for real-time execution status updates
    # This polls the database for status changes and broadcasts them to SSE clients
    # Required because subprocess executions can't broadcast SSE directly
    execution_broadcast_started = False
    try:
        from src.services.execution_broadcast_service import execution_broadcast_service

        execution_broadcast_service.start()
        execution_broadcast_started = True
        system_logger.info("Execution broadcast service started successfully")
    except Exception as e:
        system_logger.error(f"Failed to start execution broadcast service: {e}")
        # Don't raise - allow app to start without execution broadcast service

    system_logger.info("Application startup complete")

    try:
        yield
    finally:
        # Clean up running jobs during shutdown
        if db_initialized:
            system_logger.info("Application shutting down, cleaning up running jobs...")
            try:
                cleaned_jobs = (
                    await ExecutionCleanupService.cleanup_stale_jobs_on_startup()
                )
                if cleaned_jobs > 0:
                    system_logger.info(
                        f"Cleaned up {cleaned_jobs} running jobs during shutdown"
                    )
            except Exception as e:
                system_logger.error(f"Error cleaning up jobs during shutdown: {e}")

        # Stop trace broadcast service if it was started
        if "trace_broadcast_started" in locals() and trace_broadcast_started:
            system_logger.info("Stopping trace broadcast service...")
            try:
                from src.services.trace_broadcast_service import trace_broadcast_service

                trace_broadcast_service.stop()
                system_logger.info("Trace broadcast service stopped successfully.")
            except Exception as e:
                system_logger.error(f"Error stopping trace broadcast service: {e}")

        # Stop execution broadcast service if it was started
        if "execution_broadcast_started" in locals() and execution_broadcast_started:
            system_logger.info("Stopping execution broadcast service...")
            try:
                from src.services.execution_broadcast_service import (
                    execution_broadcast_service,
                )

                execution_broadcast_service.stop()
                system_logger.info("Execution broadcast service stopped successfully.")
            except Exception as e:
                system_logger.error(f"Error stopping execution broadcast service: {e}")

        # Stop HITL timeout service if it was started
        if "hitl_timeout_started" in locals() and hitl_timeout_started:
            system_logger.info("Stopping HITL timeout service...")
            try:
                from src.services.hitl_timeout_service import stop_hitl_timeout_service

                await stop_hitl_timeout_service()
                system_logger.info("HITL timeout service stopped successfully.")
            except Exception as e:
                system_logger.error(f"Error stopping HITL timeout service: {e}")

        # Stop embedding queue service if it was started
        if "embedding_queue_started" in locals() and embedding_queue_started:
            system_logger.info("Stopping embedding queue service...")
            try:
                from src.services.embedding_queue_service import embedding_queue

                # Flush any remaining items before stopping
                await embedding_queue._flush_queue()
                await embedding_queue.stop()
                system_logger.info("Embedding queue service stopped successfully.")
            except Exception as e:
                system_logger.error(f"Error stopping embedding queue service: {e}")

        # Shutdown scheduler if it was started
        if scheduler:
            system_logger.info("Shutting down scheduler...")
            try:
                await scheduler.shutdown()
                system_logger.info("Scheduler shut down successfully.")
            except Exception as e:
                system_logger.error(f"Error during scheduler shutdown: {e}")

        # Shutdown OTel App Telemetry provider (flush pending logs)
        try:
            logger_manager.shutdown_otel_app_telemetry()
        except Exception as e:
            system_logger.warning(f"Error during OTel App Telemetry shutdown: {e}")

        # Dispose database engines before event loop shuts down to prevent asyncpg loop mismatch
        try:
            from src.db.session import dispose_engines

            await dispose_engines()
            system_logger.info("Database engines disposed successfully.")
        except Exception as e:
            system_logger.warning(f"Error disposing database engines: {e}")

        system_logger.info("Application shutdown complete.")


# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.VERSION,
    lifespan=lifespan,
    # Move API docs to /api-docs
    docs_url="/api-docs" if settings.DOCS_ENABLED else None,
    redoc_url="/api-redoc" if settings.DOCS_ENABLED else None,
    openapi_url="/api-openapi.json" if settings.DOCS_ENABLED else None,
    openapi_version="3.1.0",  # Explicitly set OpenAPI version
)

# Add CORS middleware using origins from settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pure ASGI middleware (NOT BaseHTTPMiddleware) to avoid buffering
# StreamingResponse bodies, which breaks SSE streams through HTTP/2 proxies.
# See: https://github.com/encode/starlette/issues/1012
# ---------------------------------------------------------------------------


class LocalDevAuthMiddleware:
    """Pure ASGI middleware to inject auth headers for local development.

    When running locally (not in Databricks Apps), this adds default headers
    so developers can use the app without OAuth/OBO authentication.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            if b"x-forwarded-email" not in headers and b"x-auth-request-email" not in headers:
                scope["headers"] = list(scope.get("headers", [])) + [
                    (b"x-forwarded-email", os.environ.get("LOCAL_DEV_EMAIL", "admin@admin.com").encode())
                ]
                logger.debug("[LOCAL_DEV_AUTH] Injected default authentication headers for local development")
        await self.app(scope, receive, send)


app.add_middleware(LocalDevAuthMiddleware)

# Add user context middleware to extract user tokens from Databricks Apps headers
from src.utils.user_context import UserContextMiddleware  # noqa: E402

app.add_middleware(UserContextMiddleware)


# ---------------------------------------------------------------------------
# Global exception handlers
# ---------------------------------------------------------------------------
from src.core.exceptions import KasalError  # noqa: E402


@app.exception_handler(KasalError)
async def kasal_error_handler(request: Request, exc: KasalError) -> JSONResponse:
    logger.error("KasalError [%s]: %s", exc.status_code, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=exc.headers,
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    logger.warning("ValueError: %s", exc)
    return JSONResponse(status_code=400, content={"detail": str(exc)})


try:
    from pydantic import ValidationError as PydanticValidationError

    @app.exception_handler(PydanticValidationError)
    async def pydantic_validation_handler(
        request: Request, exc: PydanticValidationError
    ) -> JSONResponse:
        logger.warning("ValidationError: %s", exc)
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

except ImportError:
    pass


try:
    from sqlalchemy.exc import IntegrityError as SAIntegrityError

    @app.exception_handler(SAIntegrityError)
    async def integrity_error_handler(
        request: Request, exc: SAIntegrityError
    ) -> JSONResponse:
        logger.warning("IntegrityError: %s", exc)
        return JSONResponse(
            status_code=409, content={"detail": "Database integrity conflict"}
        )

except ImportError:
    pass


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


# Include the main API router
app.include_router(api_router, prefix=settings.API_V1_STR)


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
