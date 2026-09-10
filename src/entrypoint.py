#!/usr/bin/env python3
"""
Kasal entrypoint module.

This module serves as the entrypoint for the Kasal application when running from source.
It starts the FastAPI backend server and serves the frontend static files.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path


def _dependency_project(root: Path) -> Path:
    """Select a complete manifest pair, never an unrelated parent project."""
    for project in (root, root / "backend"):
        manifest = project / "pyproject.toml"
        lock = project / "uv.lock"
        if manifest.exists() or lock.exists():
            if not manifest.is_file() or not lock.is_file():
                raise RuntimeError(
                    f"DEP-v1: Incomplete dependency files in {project}. "
                    "Deploy both backend/pyproject.toml and backend/uv.lock together."
                )
            return project
    raise RuntimeError(
        "DEP-v1: Dependency files are missing. Deploy pyproject.toml and uv.lock "
        "beside entrypoint.py or inside backend/."
    )


def _ensure_databricks_environment() -> None:
    """Bootstrap hosted Apps before third-party imports, once per process tree."""
    entrypoint = Path(__file__).resolve()
    if not os.environ.get("DATABRICKS_APP_NAME"):
        return
    if os.environ.get("KASAL_LOCKED_ENTRYPOINT") == str(entrypoint):
        return
    root = entrypoint.parent
    try:
        project = _dependency_project(root)
        uv = shutil.which("uv")
        if not uv:
            raise RuntimeError(
                "DEP-v1: uv is unavailable on PATH. The Databricks App runtime "
                "must provide uv to install and run the declared dependencies."
            )
        print(
            f"DEP-v1: Synchronizing locked dependencies from {project}; "
            "starting in the project Python environment.",
            flush=True,
        )
        # Keep backend/static paths relative to the app root. --project selects
        # the dependency environment without changing the entrypoint's cwd.
        os.environ["KASAL_LOCKED_ENTRYPOINT"] = str(entrypoint)
        os.chdir(root)
        os.execv(
            uv,
            [
                uv,
                "run",
                "--locked",
                "--no-dev",
                "--project",
                str(project),
                "python",
                str(root / "entrypoint.py"),
                *sys.argv[1:],
            ],
        )
    except (RuntimeError, OSError) as error:
        print(f"DEP-v1: Startup failed: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1) from error


if __name__ == "__main__":
    _ensure_databricks_environment()


# Add backend directory to path FIRST
project_root = Path(__file__).parent
backend_dir = project_root / "backend"
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# Configure logging BEFORE any other imports
from src.config.logging import (  # noqa: E402 - backend path set above
    configure_early_logging,
)

configure_early_logging()

# Now import everything else
import logging  # noqa: E402 - logging configured first

from fastapi import HTTPException  # noqa: E402
from fastapi.responses import FileResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402

# Get logger after configuration
logger = logging.getLogger("kasal")


def create_parser():
    """Create argument parser for CLI options."""
    parser = argparse.ArgumentParser(description="Kasal application")
    parser.add_argument(
        "--db-type",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help="Database type to use (sqlite or postgres)",
    )
    parser.add_argument(
        "--db-url",
        help="Database URL (for postgres: postgresql://<user>:<pass>@<host>:<port>/<dbname>)",
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to run the server on"
    )
    parser.add_argument(
        "--reload", action="store_true", help="Enable auto-reload for development"
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode for all loggers (including SQLAlchemy)",
    )
    parser.add_argument(
        "--environment", choices=["dev", "prod"], help="Environment mode (dev or prod)"
    )
    return parser


class SPAMiddleware:
    """Pure ASGI middleware to serve SPA frontend for non-API routes.

    Unlike BaseHTTPMiddleware, this does NOT buffer StreamingResponse bodies,
    which is critical for SSE streams to work through HTTP/2 proxies
    (e.g. Databricks Apps).
    """

    def __init__(self, app, frontend_dir):
        self.app = app
        self.frontend_dir = (
            Path(frontend_dir) if isinstance(frontend_dir, str) else frontend_dir
        )
        self.index_path = self.frontend_dir / "index.html"

        if not self.index_path.exists():
            logger.error(f"index.html not found at {self.index_path}")
        else:
            logger.info(f"Found index.html at {self.index_path}")
            logger.info("Frontend directory contents:")
            for item in os.listdir(str(self.frontend_dir)):
                logger.info(f"  - {item}")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]

        # Skip API routes, static files, manifest, and markdown files
        if (
            path.startswith("/api/")
            or path.startswith("/api-docs")
            or path.startswith("/assets/")
            or path == "/favicon.ico"
            or path == "/manifest.json"
            or path == "/robots.txt"
            or path.endswith(".md")
            or path.endswith(".png")
            or path.endswith(".ico")
        ):
            await self.app(scope, receive, send)
            return

        # For all other routes, serve the SPA's index.html
        if self.index_path.exists():
            content = self.index_path.read_bytes()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        [b"content-type", b"text/html; charset=utf-8"],
                        [b"content-length", str(len(content)).encode()],
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": content,
                }
            )
            return

        # If index.html doesn't exist, continue with normal routing
        await self.app(scope, receive, send)


def run_app():
    """Run the Kasal application."""
    # Parse command line arguments
    parser = create_parser()
    args = parser.parse_args()

    # Enable debug mode if requested
    if args.debug:
        os.environ["KASAL_DEBUG_ALL"] = "true"
        os.environ["SQL_DEBUG"] = "true"
        os.environ["KASAL_LOG_DATABASE"] = "DEBUG"

    # Set DATABRICKS_APP_NAME for dev environment
    if args.environment == "dev":
        os.environ["KASAL_DEPLOYMENT_MODE"] = "local"
        os.environ["DATABRICKS_APP_NAME"] = "kasal-local-test"
        logger.info(
            "Development environment detected - setting DATABRICKS_APP_NAME=kasal-local-test"
        )

    logger.info("Starting Kasal application")

    # Get project root directory
    project_root = Path(__file__).parent
    logger.info(f"Project root directory: {project_root}")

    # Set environment variables for the frontend static files
    # Honor existing FRONTEND_STATIC_DIR if provided; otherwise default to src/frontend_static
    frontend_static_dir = os.environ.get("FRONTEND_STATIC_DIR") or str(
        project_root / "frontend_static"
    )
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
            # Local-dev default (postgres/postgres on localhost), assembled so
            # secret scanners don't flag a literal credential-bearing DSN.
            default_user = default_password = "postgres"
            db_url = (
                f"postgresql://{default_user}:{default_password}@localhost:5432/kasal"
            )

        os.environ["DATABASE_URL"] = db_url
        os.environ["DATABASE_URI"] = db_url  # Set both variables
    else:
        # Use SQLite (default)
        db_path = os.environ.get("SQLITE_DB_PATH", str(project_root / "kasal.db"))

        # Set all required environment variables
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
        os.environ["DATABASE_URI"] = (
            f"sqlite+aiosqlite:///{db_path}"  # Use aiosqlite for async operations
        )
        os.environ["SQLITE_DB_PATH"] = db_path  # Explicitly set the SQLITE_DB_PATH

        logger.info(f"Using SQLite database at: {db_path}")

    # Backend dir was already added at the top of the file, no need to add again

    try:
        # Serve the canonical app: its lifespan initializes the selected database
        # on Uvicorn's event loop before requests are accepted. Reconstructing it
        # dropped that lifecycle (FastAPI has no app.lifespan attribute), state,
        # and middleware; a separate init loop also hid startup failures.
        from src.main import app

        # Mount static files if they exist
        if os.path.exists(frontend_static_dir):
            # Mount Vite's assets directory
            assets_dir = os.path.join(frontend_static_dir, "assets")
            if os.path.exists(assets_dir):
                logger.info(f"Mounting /assets from {assets_dir}")
                app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

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

            # Serve common root-level assets
            logo192_png = os.path.join(frontend_static_dir, "logo192.png")
            if os.path.exists(logo192_png):

                @app.get("/logo192.png")
                async def serve_logo192():
                    return FileResponse(logo192_png)

            logo512_png = os.path.join(frontend_static_dir, "logo512.png")
            if os.path.exists(logo512_png):

                @app.get("/logo512.png")
                async def serve_logo512():
                    return FileResponse(logo512_png)

            robots_txt = os.path.join(frontend_static_dir, "robots.txt")
            if os.path.exists(robots_txt):

                @app.get("/robots.txt")
                async def serve_robots():
                    return FileResponse(robots_txt)

            # Serve Kasal icon 16px image
            kasal_icon_16 = os.path.join(frontend_static_dir, "kasal-icon-16.png")
            if os.path.exists(kasal_icon_16):

                @app.get("/kasal-icon-16.png")
                async def serve_kasal_icon_16():
                    return FileResponse(kasal_icon_16)

            # Serve Kasal icon 24px image
            kasal_icon_24 = os.path.join(frontend_static_dir, "kasal-icon-24.png")
            if os.path.exists(kasal_icon_24):

                @app.get("/kasal-icon-24.png")
                async def serve_kasal_icon_24():
                    return FileResponse(kasal_icon_24)

            # Serve Databricks logo for chat mode
            databricks_logo = os.path.join(frontend_static_dir, "databricks-logo.png")
            if os.path.exists(databricks_logo):

                @app.get("/databricks-logo.png")
                async def serve_databricks_logo():
                    return FileResponse(databricks_logo)

            # Serve markdown files from docs directory
            docs_dir = os.path.join(frontend_static_dir, "docs")
            if os.path.exists(docs_dir):

                @app.get("/docs/{filename}")
                async def serve_docs(filename: str):
                    if filename.endswith(".md"):
                        file_path = os.path.join(docs_dir, filename)
                        if os.path.exists(file_path):
                            return FileResponse(file_path, media_type="text/markdown")
                    raise HTTPException(status_code=404, detail="File not found")

        # Add middleware to serve frontend for all non-API routes
        app.add_middleware(SPAMiddleware, frontend_dir=frontend_static_dir)

        # Import uvicorn to run the app
        import uvicorn

        # Run the app with uvicorn
        logger.info(f"Starting server on port {args.port}")
        uvicorn.run(
            app, host="0.0.0.0", port=args.port, reload=args.reload, log_level="info"
        )
    except Exception as e:
        logger.error(f"Error starting Kasal application: {e}")
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    run_app()
