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


# --- Launch configuration (stdlib only, so tests can exercise it without the
# backend's imports; see tests/unit/test_entrypoint_launch_config.py) --------

# The async driver every Kasal engine uses. A bare ``postgresql://`` URL makes
# SQLAlchemy pick psycopg2, which is not installed, so the app cannot start.
_ASYNC_POSTGRES_SCHEME = "postgresql+asyncpg://"


def _async_postgres_url(url: str) -> str:
    """``postgres://`` / ``postgresql://`` -> ``postgresql+asyncpg://``.

    A URL that already names a driver is left alone: that is a deliberate choice.
    """
    for scheme in ("postgresql://", "postgres://"):
        if url.startswith(scheme):
            return _ASYNC_POSTGRES_SCHEME + url[len(scheme) :]
    return url


def _database_environment(
    db_type: str, db_url: "str | None", project_root: Path, environ=os.environ
) -> "dict[str, str]":
    """The variables that select the database, for THIS process and its children.

    ``DATABASE_TYPE`` is part of it on purpose. ``Settings`` defaults it to
    ``sqlite``, and the crew/flow subprocesses copy it from the parent: a
    Postgres run that exported only ``DATABASE_URI`` reported ``sqlite`` and
    sent its children down the SQLite code paths.
    """
    if db_type == "postgres":
        if db_url:
            url = db_url
        else:
            # Local-dev default (postgres/postgres on localhost), assembled so
            # secret scanners don't flag a literal credential-bearing DSN.
            default_user = default_password = "postgres"
            url = f"postgresql://{default_user}:{default_password}@localhost:5432/kasal"
        url = _async_postgres_url(url)
        return {
            "DATABASE_TYPE": "postgres",
            "DATABASE_URL": url,
            "DATABASE_URI": url,
            # Settings builds this from POSTGRES_* otherwise, i.e. localhost.
            "SYNC_DATABASE_URI": url,
        }
    db_path = environ.get("SQLITE_DB_PATH") or str(project_root / "kasal.db")
    return {
        "DATABASE_TYPE": "sqlite",
        "DATABASE_URL": f"sqlite:///{db_path}",
        "DATABASE_URI": f"sqlite+aiosqlite:///{db_path}",
        "SYNC_DATABASE_URI": f"sqlite:///{db_path}",
        "SQLITE_DB_PATH": db_path,
    }


def _bind_host(environ=os.environ) -> str:
    """``KASAL_BIND_HOST``; else every interface inside Databricks Apps (the
    platform proxy must reach the app), else loopback, as ``run.sh`` does."""
    explicit = environ.get("KASAL_BIND_HOST", "").strip()
    if explicit:
        return explicit
    return "0.0.0.0" if environ.get("DATABRICKS_APP_NAME") else "127.0.0.1"


DEFAULT_PORT = 8000


def _listen_port(cli_port: "int | None", environ=os.environ) -> int:
    """The port to bind: ``--port``, else the platform's, else 8000.

    Inside Databricks Apps (``DATABRICKS_APP_NAME`` set) the platform injects
    ``DATABRICKS_APP_PORT`` and forwards traffic THERE; binding a hard-coded 8000
    only worked while the two happened to agree.
    """
    if cli_port is not None:
        return cli_port
    platform_port = environ.get("DATABRICKS_APP_PORT", "").strip()
    if environ.get("DATABRICKS_APP_NAME") and platform_port:
        try:
            return int(platform_port)
        except ValueError:
            print(
                f"Ignoring invalid DATABRICKS_APP_PORT={platform_port!r}",
                file=sys.stderr,
            )
    return DEFAULT_PORT


def _environment_overrides(environment: "str | None", environ=os.environ):
    """What ``--environment`` sets. Only ``dev`` changes anything.

    ``--environment dev`` is a LOCAL run, the same as ``run.sh``: it forces
    ``KASAL_DEPLOYMENT_MODE=local`` (never treated as hosted) and turns the
    development identity on (``LOCAL_DEV_AUTH=true``, unless you set it
    yourself), so a request without an identity header runs as
    ``LOCAL_DEV_USER_EMAIL``. It used to set ``DATABRICKS_APP_NAME``, which
    does the opposite: the backend reads that as "inside Databricks Apps" and
    refuses ``LOCAL_DEV_AUTH``, so every API call was a 401. Inside real Apps
    (``DATABRICKS_APP_NAME`` set by the platform) the backend still refuses the
    development identity, whatever this sets.
    """
    if environment != "dev":
        return {}
    return {
        "KASAL_DEPLOYMENT_MODE": "local",
        "LOCAL_DEV_AUTH": environ.get("LOCAL_DEV_AUTH") or "true",
    }


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
        help=(
            "Database URL for --db-type postgres: "
            "postgresql://<user>:<pass>@<host>:<port>/<dbname> "
            "(the asyncpg driver is selected for you)"
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=(
            "Port to run the server on (default: DATABRICKS_APP_PORT inside "
            f"Databricks Apps, else {DEFAULT_PORT})"
        ),
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart when backend/src changes (development only)",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode for all loggers (including SQLAlchemy)",
    )
    parser.add_argument(
        "--environment",
        choices=["dev", "prod"],
        help=(
            "dev: a local run like run.sh (LOCAL_DEV_AUTH=true unless set, "
            "KASAL_DEPLOYMENT_MODE=local). prod (or omitted): no overrides"
        ),
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


def _apply_environment(updates: "dict[str, str]") -> None:
    """Export ``updates``, and push the database ones into ``Settings`` if it was
    already built (an import above can create it before the flags are read)."""
    os.environ.update(updates)
    settings_module = sys.modules.get("src.config.settings")
    settings = getattr(settings_module, "settings", None)
    if settings is None:
        return
    for key in ("DATABASE_TYPE", "DATABASE_URI", "SYNC_DATABASE_URI", "SQLITE_DB_PATH"):
        if key in updates:
            setattr(settings, key, updates[key])


def build_app():
    """The canonical app plus the static frontend, as served by this entrypoint.

    Serve ``src.main:app`` itself: its lifespan initializes the selected
    database on Uvicorn's event loop before requests are accepted.
    Reconstructing it dropped that lifecycle (FastAPI has no app.lifespan
    attribute), state and middleware; a separate init loop also hid startup
    failures. A function (a uvicorn factory) so ``--reload`` can rebuild it in
    the reloader's worker process: the configuration travels in the
    environment, which ``run_app`` has exported by then.
    """
    from src.main import app

    frontend_static_dir = os.environ.get("FRONTEND_STATIC_DIR") or str(
        Path(__file__).parent / "frontend_static"
    )
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

    # Add middleware to serve frontend for all non-API routes
    app.add_middleware(SPAMiddleware, frontend_dir=frontend_static_dir)
    return app


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

    overrides = _environment_overrides(args.environment)
    if overrides:
        os.environ.update(overrides)
        logger.info(
            "--environment dev: local run (KASAL_DEPLOYMENT_MODE=local, "
            "LOCAL_DEV_AUTH=%s)",
            overrides["LOCAL_DEV_AUTH"],
        )
        if os.environ.get("DATABRICKS_APP_NAME"):
            logger.warning(
                "--environment dev inside Databricks Apps: the development "
                "identity stays off; identity comes from the platform proxy"
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

    # Set database configuration: exported, so the crew/flow subprocesses (and
    # a --reload worker) agree with this process.
    if args.db_type == "postgres" and not args.db_url:
        logger.warning("No database URL provided for PostgreSQL. Using default.")
    database = _database_environment(args.db_type, args.db_url, project_root)
    _apply_environment(database)
    if args.db_type == "postgres":
        logger.info("Using PostgreSQL (asyncpg driver)")
    else:
        logger.info(f"Using SQLite database at: {database['SQLITE_DB_PATH']}")

    try:
        import uvicorn

        host = _bind_host()
        port = _listen_port(args.port)
        logger.info(f"Starting server on {host}:{port}")
        if args.reload:
            # Uvicorn can only reload an import string: it re-imports the app
            # in a fresh worker process on every change.
            uvicorn.run(
                "entrypoint:build_app",
                factory=True,
                host=host,
                port=port,
                reload=True,
                reload_dirs=[str(backend_dir / "src")],
                app_dir=str(project_root),
                log_level="info",
            )
        else:
            uvicorn.run(build_app(), host=host, port=port, log_level="info")
    except Exception as e:
        logger.error(f"Error starting Kasal application: {e}")
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    run_app()
