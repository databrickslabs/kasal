#!/usr/bin/env python3
"""
Direct deployment script for Kasal application.

Deploys backend source, frontend source, and docs to Databricks Apps.
The frontend ships as SOURCE together with the root package.json; Databricks
Apps runs npm install + npm run build during deployment (remote build).

Scopes: by default BOTH frontend and backend are uploaded. Pass --frontend to
upload only the frontend (and docs), or --backend to upload only the backend.
Upload uses `databricks sync` per component — full upload by default, pass
--diff for an incremental (changed-files-only) sync.
"""

import os
import shutil
import subprocess
import sys
import logging
import argparse
import json
import tempfile
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeploymentMode, App, AppDeployment
from databricks.sdk.service.workspace import ImportFormat

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("deploy")

def custom_ignore_function(excluded_dirs, excluded_patterns):
    """Create a robust ignore function that excludes specific directories and patterns."""
    def _ignore(directory, contents):
        ignored = []
        for item in contents:
            # Ignore specific directory names
            if item in excluded_dirs:
                ignored.append(item)
                logger.debug(f"Ignoring directory: {item}")
            # Ignore patterns
            elif any(Path(item).match(pattern) for pattern in excluded_patterns):
                ignored.append(item)
                logger.debug(f"Ignoring pattern match: {item}")
        return ignored
    return _ignore

def get_desired_oauth_scopes(exclude_dataplane=True):
    """Return the OAuth scopes the app should have."""
    desired_scopes = [
        # SQL related scopes
        "sql",
        "sql.alerts",
        "sql.alerts-legacy",
        "sql.dashboards",
        "sql.data-sources",
        "sql.dbsql-permissions",
        "sql.queries",
        "sql.queries-legacy",
        "sql.query-history",
        "sql.statement-execution",
        "sql.warehouses",

        # Vector Search scopes - CRITICAL for index creation
        "vectorsearch.vector-search-endpoints",
        "vectorsearch.vector-search-indexes",

        # Serving endpoints (excluding data-plane if requested)
        "serving.serving-endpoints",

        # Files
        "files.files",

        # Dashboards/Genie
        "dashboards.genie",

        # Unity Catalog - CRITICAL for catalog operations
        "catalog.connections",
        "catalog.catalogs:read",
        "catalog.tables:read",
        "catalog.schemas:read",
    ]
    if not exclude_dataplane:
        desired_scopes.append("serving.serving-endpoints-data-plane")
    return desired_scopes

def configure_oauth_scopes(app_name, exclude_dataplane=True):
    """Configure OAuth scopes for the Databricks app

    Args:
        app_name: Name of the Databricks app
        exclude_dataplane: If True, excludes the problematic serving-endpoints-data-plane scope

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Configuring OAuth scopes for app: {app_name}")

        desired_scopes = get_desired_oauth_scopes(exclude_dataplane=exclude_dataplane)
        if not exclude_dataplane:
            logger.warning("Including serving-endpoints-data-plane scope (may cause issues)")
        else:
            logger.info("Excluding serving-endpoints-data-plane scope (known to cause issues)")

        logger.info(f"Configuring {len(desired_scopes)} OAuth scopes")

        # Prepare the JSON payload
        payload = {
            "user_api_scopes": desired_scopes
        }

        logger.info("Executing OAuth scope configuration...")

        # Execute the API call using databricks CLI
        cmd = [
            "databricks", "api", "patch",
            f"/api/2.0/apps/{app_name}",
            "--json", json.dumps(payload),
            "-o", "json"
        ]

        # Run the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )

        if result.returncode == 0:
            logger.info("✅ OAuth scopes configured successfully")

            try:
                response_data = json.loads(result.stdout)
                if "user_api_scopes" in response_data:
                    configured_scopes = response_data["user_api_scopes"]
                    logger.info(f"Successfully configured {len(configured_scopes)} scopes")

                    # Log scope categories
                    sql_scopes = [s for s in configured_scopes if s.startswith("sql")]
                    vector_scopes = [s for s in configured_scopes if s.startswith("vectorsearch")]
                    serving_scopes = [s for s in configured_scopes if s.startswith("serving")]

                    if sql_scopes:
                        logger.info(f"  SQL scopes configured: {len(sql_scopes)}")
                    if vector_scopes:
                        logger.info(f"  Vector Search scopes configured: {len(vector_scopes)}")
                    if serving_scopes:
                        logger.info(f"  Serving scopes configured: {len(serving_scopes)}")

            except json.JSONDecodeError:
                logger.debug(f"OAuth configuration response: {result.stdout}")

            return True
        else:
            logger.error(f"OAuth scope configuration failed with exit code {result.returncode}")

            # Log actual error for debugging
            if result.stderr:
                logger.error(f"Scope error: {result.stderr.strip()}")
            if result.stdout:
                logger.error(f"Scope output: {result.stdout.strip()}")
            if "is not a valid scope" in (result.stderr or ""):
                logger.warning("Invalid scope detected. The app may require manual scope configuration.")

            return False

    except subprocess.CalledProcessError as e:
        logger.error(f"OAuth scope configuration command execution failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error configuring OAuth scopes: {e}")
        return False

def deploy_source_to_databricks(
    app_name="kasal",
    user_name=None,
    workspace_dir=None,
    profile=None,
    host=None,
    token=None,
    description=None,
    oauth_scopes=None,
    config_template=None,
    api_url=None,
    configure_oauth=True,
    exclude_dataplane=True,
    full_sync=True,
    scope="all"
):
    """Deploy source code to Databricks Apps"""
    root_dir = Path(os.path.dirname(os.path.abspath(__file__)))

    logger.info(f"Deploying source code from: {root_dir}")

    # Set default workspace directory if not provided
    if user_name is None:
        user_name = os.environ.get("USER", "default_user")
    
    if not workspace_dir:
        workspace_dir = f"/Workspace/Users/{user_name}/{app_name}"
    
    # Connect to Databricks
    if profile:
        logger.info(f"Connecting to Databricks using profile: {profile}")
        client = WorkspaceClient(profile=profile)
    elif host and token:
        logger.info(f"Connecting to Databricks using host: {host}")
        client = WorkspaceClient(host=host, token=token)
    else:
        logger.info(f"Connecting to Databricks using default configuration")
        client = WorkspaceClient()
    
    try:
        me = client.current_user.me()
        logger.info(f"Connected to Databricks as {me.user_name}")
    except Exception as e:
        logger.warning(f"Could not verify identity (proceeding anyway): {e}")
        logger.info(f"Connecting to Databricks at {client.config.host}")

    # Frontend ships as SOURCE: Databricks Apps detects the root package.json
    # and runs npm install + npm run build during deployment.
    ship_frontend = scope in ("all", "frontend")
    ship_backend = scope in ("all", "backend")
    if ship_frontend:
        if not (root_dir / "frontend").exists():
            logger.error("frontend/ directory not found. Cannot deploy frontend.")
            raise FileNotFoundError("frontend/ directory not found")
        if not (root_dir / "package.json").exists():
            logger.error("package.json not found. Required for the npm build on Databricks Apps.")
            raise FileNotFoundError("package.json not found")

    # Check that docs directory exists
    docs_dir = root_dir / "docs"
    if not docs_dir.exists():
        logger.warning("docs/ directory not found. Continuing without docs.")

    # Verify app.yaml exists
    app_yaml_path = root_dir / "app.yaml"
    if not app_yaml_path.exists():
        logger.error("app.yaml not found in root directory. Please ensure app.yaml exists.")
        raise FileNotFoundError("app.yaml not found")
    
    logger.info(f"Using existing app.yaml at {app_yaml_path}")
    
    # uv-native dependency management.
    # The app's Python dependencies are defined in backend/pyproject.toml and
    # locked in backend/uv.lock. We copy BOTH to the bundle root below so the
    # Databricks Apps build runs `uv sync` for a fully reproducible install.
    # We deliberately do NOT generate or ship a requirements.txt — if one exists
    # at the bundle root it takes precedence and bypasses uv (see the Databricks
    # Apps "Manage dependencies" docs).
    pyproject_src = root_dir / "backend" / "pyproject.toml"
    uvlock_src = root_dir / "backend" / "uv.lock"
    if not pyproject_src.exists() or not uvlock_src.exists():
        logger.error(
            "uv manifests not found: backend/pyproject.toml and backend/uv.lock "
            "are required for the uv-based deploy. Run `cd src/backend && uv lock`."
        )
        raise FileNotFoundError("backend/pyproject.toml or backend/uv.lock not found")
    logger.info("Using uv (pyproject.toml + uv.lock) for dependency management")

    try:
        # Check if app exists, create if not
        try:
            app_exists = False
            app_info = None
            logger.info(f"Checking if app {app_name} exists")

            try:
                # Try to get the app
                app_info = client.apps.get(name=app_name)
                app_exists = True
                logger.info(f"App {app_name} already exists")
            except Exception as get_err:
                # App doesn't exist
                logger.info(f"App {app_name} does not exist (will create): {get_err}")
                app_exists = False
            
            if not app_exists:
                logger.info(f"Creating app: {app_name}")
                app_description = description if description else f"{app_name} application"

                # Create an App object first, then pass it to create_and_wait
                app_obj = App(name=app_name)
                if description:
                    app_obj.description = description

                app = client.apps.create_and_wait(app=app_obj)
                logger.info(f"Created app: {app_name}")

                # Configure OAuth scopes for the newly created app
                if configure_oauth:
                    logger.info("Configuring OAuth scopes for the new app...")
                    oauth_success = configure_oauth_scopes(app_name, exclude_dataplane=exclude_dataplane)
                    if not oauth_success:
                        logger.warning("OAuth scope configuration failed, but continuing with deployment")
            else:
                # Configure OAuth scopes for existing app if requested,
                # skipping the PATCH when the app already has the desired set.
                if configure_oauth:
                    desired = set(get_desired_oauth_scopes(exclude_dataplane=exclude_dataplane))
                    current = set(getattr(app_info, "user_api_scopes", None) or [])
                    if current == desired:
                        logger.info("OAuth scopes already up to date, skipping configuration")
                    else:
                        logger.info("Updating OAuth scopes for existing app...")
                        oauth_success = configure_oauth_scopes(app_name, exclude_dataplane=exclude_dataplane)
                        if not oauth_success:
                            logger.warning("OAuth scope configuration failed, but continuing with deployment")

        except Exception as e:
            logger.error(f"Error checking/creating app: {e}")
            raise
        
        # Create the deployment bundle with only the files we need.
        # IMPORTANT: the bundle lives OUTSIDE the git repo — `databricks sync`
        # honors the repository's .gitignore, which ignores databricksdist/ and
        # frontend_static/, so an in-repo bundle would be silently skipped.
        # The path is stable per app so sync's incremental snapshot keeps working.
        try:
            databricks_dist = Path(tempfile.gettempdir()) / f"kasal-databricksdist-{app_name}"
            logger.info(f"Creating clean databricks deployment directory: {databricks_dist}")
            
            # Remove and recreate databricksdist directory
            if databricks_dist.exists():
                shutil.rmtree(databricks_dist)
            databricks_dist.mkdir()
            
            # Copy backend: WHITELIST — the deployed app only needs backend/src.
            # Everything else in src/backend (tests, migrations, .venv, runtime
            # artifacts like kasal_default_* LanceDB dirs, *.db, caches) is dev
            # junk that must never ship. migrations/ (alembic) stays dev-only:
            # the app creates and self-heals its schema at startup
            # (db/session.py create_all + _ensure_*_columns).
            if ship_backend:
                logger.info("Copying backend/src (whitelist mode)...")
                backend_src = root_dir / "backend"
                backend_dst = databricks_dist / "backend"
                if not (backend_src / "src").exists():
                    logger.error("backend/src folder not found!")
                    raise FileNotFoundError("backend/src folder not found")
                backend_excluded_dirs = {
                    '__pycache__', '.pytest_cache', '.mypy_cache', 'node_modules', 'logs', 'tmp'
                }
                backend_excluded_patterns = [
                    '*.pyc', '*.pyo', '*.log', '*.db', '*.db-shm', '*.db-wal',
                    '*.backup', '.coverage', '.env', '.gitignore', '.DS_Store'
                ]
                backend_dst.mkdir()
                shutil.copytree(
                    backend_src / "src",
                    backend_dst / "src",
                    ignore=custom_ignore_function(backend_excluded_dirs, backend_excluded_patterns)
                )
                logger.info("Copied backend/src (only src/ ships; dev/test/runtime artifacts stay local)")

            if ship_frontend:
                # Frontend source; Databricks Apps runs the npm lifecycle from
                # the root package.json during deployment.
                logger.info("Copying frontend source folder...")
                frontend_excluded_dirs = {'node_modules', 'dist', 'coverage', 'build', '.git', '.benchmarks'}
                frontend_excluded_patterns = [
                    '.env.local', '.env.development.local', '.env.test.local', '.env.production.local',
                    'package-lock.json', '*.tsbuildinfo', '.DS_Store'
                ]
                shutil.copytree(
                    root_dir / "frontend",
                    databricks_dist / "frontend",
                    ignore=custom_ignore_function(frontend_excluded_dirs, frontend_excluded_patterns)
                )
                logger.info(f"Copied frontend source folder (excluding: {frontend_excluded_dirs})")

                # Docs ship with the frontend: the npm prebuild copies docs/*.md
                # into frontend/public/docs, and the app serves them from there.
                docs_src = root_dir / "docs"
                if docs_src.exists():
                    logger.info("Copying docs folder...")
                    shutil.copytree(
                        docs_src,
                        databricks_dist / "docs",
                        ignore=custom_ignore_function({'archive', '__pycache__'}, ['*.pyc', '.DS_Store'])
                    )
                    logger.info("Copied docs folder")
                else:
                    logger.warning("docs/ folder not found, skipping")

            # Root files always ship (tiny): the app needs all of them whatever
            # the scope. package.json triggers the npm build on Databricks Apps;
            # pyproject.toml + uv.lock at the bundle root make the build run
            # `uv sync` (we never ship requirements.txt — it would take
            # precedence and bypass uv).
            root_files = ["app.yaml", "entrypoint.py", "package.json"]
            for file_name in root_files:
                src_file = root_dir / file_name
                if src_file.exists():
                    shutil.copy2(src_file, databricks_dist / file_name)
                    logger.info(f"Copied {file_name}")
                else:
                    logger.warning(f"{file_name} not found, skipping")
            for manifest in ("pyproject.toml", "uv.lock"):
                shutil.copy2(root_dir / "backend" / manifest, databricks_dist / manifest)
                logger.info(f"Copied {manifest} to bundle root")

            logger.info(f"Uploading deployment to workspace: {workspace_dir}")
            logger.info(f"Uploading from: {databricks_dist} (scope: {scope})")
            logger.info("Proceeding with upload...")

            # Remove stale workspace artifacts: requirements.txt would override
            # uv; frontend_static/ is a leftover from the old prebuilt-assets
            # deploy mode (the platform npm build creates its own copy inside
            # the deployment snapshot, not in the workspace).
            stale_paths = [(f"{workspace_dir}/requirements.txt", False),
                           (f"{workspace_dir}/frontend_static", True)]
            for stale_path, recursive in stale_paths:
                try:
                    client.workspace.delete(stale_path, recursive=recursive)
                    logger.info(f"Removed stale workspace artifact: {stale_path}")
                except Exception:
                    pass  # not present — nothing to clean

            # Prune remote entries that are not part of the local bundle.
            # `databricks sync` never deletes files it didn't upload itself, so
            # junk from older deploys (.ruff_cache, kasal_default_* memory dirs,
            # .DS_Store, tests/, migrations/, ...) would otherwise live in the
            # workspace — and the deployment snapshot — forever.
            def prune_remote_dir(remote_dir, local_dir):
                try:
                    remote_entries = list(client.workspace.list(remote_dir))
                except Exception:
                    return  # remote dir doesn't exist yet
                local_names = {p.name for p in local_dir.iterdir()} if local_dir.exists() else set()
                for entry in remote_entries:
                    entry_path = entry.path or ""
                    name = entry_path.rsplit("/", 1)[-1]
                    if entry_path and name not in local_names:
                        try:
                            client.workspace.delete(entry_path, recursive=True)
                            logger.info(f"Pruned remote leftover: {entry_path}")
                        except Exception as prune_err:
                            logger.warning(f"Could not prune {entry_path}: {prune_err}")

            if ship_backend:
                prune_remote_dir(f"{workspace_dir}/backend", databricks_dist / "backend")
            if ship_frontend:
                prune_remote_dir(f"{workspace_dir}/frontend", databricks_dist / "frontend")
                prune_remote_dir(f"{workspace_dir}/docs", databricks_dist / "docs")

            # Upload each component with `databricks sync` (parallel transfers).
            # Full upload by default; with --diff the per-pair snapshot (under
            # ~/.databricks, keyed on local+remote path) skips unchanged files —
            # copy2 preserves source mtimes, so the snapshot stays valid even
            # though the bundle directory is recreated each run.
            def run_sync(local_dir, remote_dir):
                sync_cmd = ["databricks", "sync", str(local_dir), remote_dir]
                if full_sync:
                    sync_cmd.append("--full")
                if profile is not None:
                    sync_cmd.extend(["--profile", profile])
                logger.info(f"Syncing {local_dir.name}/ to {remote_dir}{' (full)' if full_sync else ' (diff)'}")
                result = subprocess.run(sync_cmd, check=True, capture_output=True, text=True)
                if result.stderr:
                    logger.debug(f"sync output: {result.stderr.strip()}")

            if ship_backend:
                run_sync(databricks_dist / "backend", f"{workspace_dir}/backend")
            if ship_frontend:
                run_sync(databricks_dist / "frontend", f"{workspace_dir}/frontend")
                if (databricks_dist / "docs").exists():
                    run_sync(databricks_dist / "docs", f"{workspace_dir}/docs")

            # Root files go up individually via the SDK (avoids any .py→notebook
            # conversion and keeps them out of the directory snapshots).
            for file_name in root_files + ["pyproject.toml", "uv.lock"]:
                file_path = databricks_dist / file_name
                if file_path.exists():
                    with open(file_path, "rb") as f:
                        client.workspace.upload(
                            f"{workspace_dir}/{file_name}", f,
                            format=ImportFormat.AUTO, overwrite=True
                        )
                    logger.info(f"Uploaded {file_name}")

            # Verify what actually landed — sync can silently skip files
            # (e.g. gitignore rules), so trust but verify.
            try:
                remote_top = sorted(
                    (e.path or "").rsplit("/", 1)[-1] for e in client.workspace.list(workspace_dir)
                )
                logger.info(f"Workspace now contains: {remote_top}")
                for expected in (["frontend", "package.json"] if ship_frontend else []) + (["backend"] if ship_backend else []):
                    if expected not in remote_top:
                        logger.error(
                            f"{expected} is MISSING from the workspace after sync — "
                            "check the `databricks sync` output."
                        )
            except Exception as verify_err:
                logger.warning(f"Could not verify workspace contents: {verify_err}")

            logger.info("All files uploaded successfully")

            # Clean up databricksdist directory
            logger.info("Cleaning up databricksdist directory")
            shutil.rmtree(databricks_dist)

            logger.info("✅ Upload completed successfully!")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Error uploading source code: {e}")
            if e.stdout:
                logger.error(f"Stdout: {e.stdout}")
            if e.stderr:
                logger.error(f"Stderr: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error during upload: {e}")
            raise
        
        # Now deploy the app using the uploaded files
        logger.info("=" * 60)
        logger.info("🚀 Starting app deployment...")
        logger.info("=" * 60)
        try:
            logger.info(f"Deploying app {app_name} from {workspace_dir}")
            
            # Create an AppDeployment object to use with deploy
            try:
                logger.info(f"Creating AppDeployment object with workspace_dir={workspace_dir}")
                app_deployment = AppDeployment(
                    source_code_path=workspace_dir,
                    mode=AppDeploymentMode.SNAPSHOT
                )
                logger.info(f"AppDeployment object created successfully")
            except Exception as e:
                logger.error(f"Error creating AppDeployment object: {e}")
                raise
            
            # Deploy the app. If another deployment is already in progress
            # (e.g. a previous run still building), wait for it and retry.
            deadline = time.time() + 1800  # give an in-flight deployment up to 30 min
            while True:
                try:
                    logger.info("Deploying application")
                    waiter = client.apps.deploy(
                        app_name=app_name,
                        app_deployment=app_deployment
                    )
                    result = waiter.result()
                    deployment_id = result.deployment_id
                    logger.info(f"Deployment created with ID: {deployment_id}")
                    break
                except Exception as e1:
                    if "active deployment in progress" in str(e1) and time.time() < deadline:
                        logger.info("Another deployment is in progress — waiting 20s before retrying...")
                        time.sleep(20)
                        continue
                    logger.error(f"Deployment failed with error type: {type(e1)}")
                    logger.error(f"Deployment error: {e1}")
                    return False
            
            # waiter.result() above already blocked until the deployment
            # reached a terminal state — start the app immediately.
            # Start the app
            try:
                logger.info(f"Starting app: {app_name}")
                client.apps.start(app_name)
                logger.info(f"App started. Check the app URL: {client.config.host}#apps/{app_name}")
                return True
            except Exception as start_error:
                if "compute is in ACTIVE state" in str(start_error):
                    logger.info("App is already running - deployment successful!")
                    return True
                logger.error(f"Error starting app: {start_error}")
                try:
                    app_info = client.apps.get(name=app_name)
                    logger.info(f"App info: {app_info}")
                    if hasattr(app_info, 'state'):
                        logger.info(f"App state: {app_info.state}")
                except Exception as info_error:
                    logger.error(f"Error getting app info: {info_error}")
                return False
        
        except Exception as e:
            logger.error(f"Error during deployment: {e}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
    except Exception as e:
        logger.error(f"Error during deployment: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Deploy source code to Databricks Apps")
    parser.add_argument("--app-name", default="kasal", required=True,
                        help="Name for the Databricks App (lowercase with hyphens only)")
    parser.add_argument("--user-name", required=True,
                        help="User name for workspace path (e.g., user@example.com)")
    parser.add_argument("--workspace-dir",
                        help="Workspace directory to upload files (default: /Workspace/Users/<user-name>/<app-name>)")
    parser.add_argument("--profile", help="Databricks CLI profile to use")
    parser.add_argument("--host", help="Databricks host URL")
    parser.add_argument("--token", help="Databricks API token")
    parser.add_argument("--description", help="Description for the app")
    parser.add_argument("--oauth-scopes", nargs="*",
                        help="Custom OAuth scopes for the app (default: comprehensive set)")
    parser.add_argument("--config-template",
                        help="Path to app.yaml template file (default: use built-in template)")
    parser.add_argument("--api-url",
                        help="API URL to use in the frontend build (e.g. https://kasal-xxx.aws.databricksapps.com/api/v1)")
    parser.add_argument("--configure-oauth", action="store_true", default=True,
                        help="Configure OAuth scopes during deployment (default: True)")
    parser.add_argument("--no-configure-oauth", dest="configure_oauth", action="store_false",
                        help="Skip OAuth scope configuration")
    parser.add_argument("--include-dataplane", action="store_true", default=False,
                        help="Include the problematic serving-endpoints-data-plane scope (not recommended)")
    parser.add_argument("--frontend", action="store_true", default=False,
                        help="Deploy only the frontend (and docs)")
    parser.add_argument("--backend", action="store_true", default=False,
                        help="Deploy only the backend")
    parser.add_argument("--diff", action="store_true", default=False,
                        help="Incremental upload — sync only changed files (default: full upload)")

    args = parser.parse_args()
    
    # Validate app name (lowercase letters, numbers, and hyphens only)
    import re
    if not re.match(r'^[a-z0-9-]+$', args.app_name):
        logger.error("App name must contain only lowercase letters, numbers, and hyphens")
        sys.exit(1)
    
    # Resolve deploy scope: default is everything; --frontend / --backend narrow
    # it. Passing both is the same as the default. The frontend is built ON
    # Databricks Apps (npm lifecycle from package.json) — no local build.
    if args.frontend and not args.backend:
        scope = "frontend"
    elif args.backend and not args.frontend:
        scope = "backend"
    else:
        scope = "all"
    logger.info(f"Deploy scope: {scope} (frontend builds on Databricks Apps during deployment)")

    try:
        success = deploy_source_to_databricks(
            app_name=args.app_name,
            user_name=args.user_name,
            workspace_dir=args.workspace_dir,
            profile=args.profile,
            host=args.host,
            token=args.token,
            description=args.description,
            oauth_scopes=getattr(args, 'oauth_scopes', None),
            config_template=getattr(args, 'config_template', None),
            api_url=args.api_url,
            configure_oauth=args.configure_oauth,
            exclude_dataplane=not args.include_dataplane,
            full_sync=not args.diff,
            scope=scope
        )
        
        if success:
            logger.info("Deployment completed successfully")
            sys.exit(0)
        else:
            logger.error("Deployment failed")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error during deployment: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
