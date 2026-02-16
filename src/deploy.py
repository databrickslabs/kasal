#!/usr/bin/env python3
"""
Direct deployment script for Kasal application.

Deploys backend source, frontend source, and docs to Databricks Apps.
Frontend is built on Databricks Apps via npm lifecycle hooks in package.json.
Uses hybrid upload: SDK for root files (avoids .py→notebook conversion), import-dir for directories.
"""

import os
import shutil
import subprocess
import sys
import logging
import time
import argparse
import json
from pathlib import Path
from datetime import datetime

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

def clean_python_cache(root_dir):
    """Clean Python cache files and directories"""
    logger.info("Cleaning Python cache files...")
    
    # Clean __pycache__ directories
    cache_dirs = list(root_dir.rglob("__pycache__"))
    for cache_dir in cache_dirs:
        try:
            shutil.rmtree(cache_dir)
            logger.debug(f"Removed cache directory: {cache_dir}")
        except Exception as e:
            logger.warning(f"Failed to remove {cache_dir}: {e}")
    
    # Clean .pyc and .pyo files
    pyc_files = list(root_dir.rglob("*.pyc")) + list(root_dir.rglob("*.pyo"))
    for pyc_file in pyc_files:
        try:
            pyc_file.unlink()
            logger.debug(f"Removed cache file: {pyc_file}")
        except Exception as e:
            logger.warning(f"Failed to remove {pyc_file}: {e}")
    
    # Clean .pytest_cache directories
    pytest_cache_dirs = list(root_dir.rglob(".pytest_cache"))
    for cache_dir in pytest_cache_dirs:
        try:
            shutil.rmtree(cache_dir)
            logger.debug(f"Removed pytest cache: {cache_dir}")
        except Exception as e:
            logger.warning(f"Failed to remove {cache_dir}: {e}")
    
    logger.info(f"Cache cleaning completed. Removed {len(cache_dirs)} __pycache__ directories, {len(pyc_files)} .pyc/.pyo files, and {len(pytest_cache_dirs)} .pytest_cache directories")

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

        # Define scopes - excluding the problematic dataplane scope by default
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

            # Unity Catalog - CRITICAL for volumes and catalog operations
            "catalog.connections",
            "catalog.catalogs:read",
            "catalog.tables:read",
            "catalog.schemas:read",
            "catalog.volumes",
        ]

        # Add data-plane scope only if explicitly requested
        if not exclude_dataplane:
            logger.warning("Including serving-endpoints-data-plane scope (may cause issues)")
            desired_scopes.append("serving.serving-endpoints-data-plane")
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

            # Check for specific scope errors
            if result.stderr and "is not a valid scope" in result.stderr:
                logger.warning("Invalid scope detected. The app may require manual scope configuration.")
            else:
                logger.debug(f"Error output: {result.stderr}")
                logger.debug(f"Standard output: {result.stdout}")

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
    exclude_dataplane=True
):
    """Deploy source code to Databricks Apps"""
    root_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    
    logger.info(f"Deploying source code from: {root_dir}")
    
    # Clean Python cache before deployment
    clean_python_cache(root_dir)
    
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
        # Test connection
        me = client.current_user.me()
        logger.info(f"Connected to Databricks as {me.user_name}")
    except Exception as e:
        logger.error(f"Failed to connect to Databricks: {e}")
        raise
    
    # Check that frontend source directory exists (built on Databricks Apps)
    frontend_src_dir = root_dir / "frontend"
    if not frontend_src_dir.exists():
        logger.error("frontend/ directory not found. Cannot deploy without frontend source.")
        raise FileNotFoundError("frontend/ directory not found")

    # Check that docs directory exists
    docs_dir = root_dir / "docs"
    if not docs_dir.exists():
        logger.warning("docs/ directory not found. Continuing without docs.")

    # Check that root package.json exists (needed for Databricks Apps build)
    root_package_json = root_dir / "package.json"
    if not root_package_json.exists():
        logger.error("package.json not found. This file is required for building frontend on Databricks Apps.")
        raise FileNotFoundError("package.json not found")

    # Verify app.yaml exists
    app_yaml_path = root_dir / "app.yaml"
    if not app_yaml_path.exists():
        logger.error("app.yaml not found in root directory. Please ensure app.yaml exists.")
        raise FileNotFoundError("app.yaml not found")
    
    logger.info(f"Using existing app.yaml at {app_yaml_path}")
    
    # Create requirements.txt
    requirements_path = root_dir / "requirements.txt"
    if not requirements_path.exists():
        logger.info("Creating requirements.txt")
        requirements_content = """fastapi>=0.110.0
uvicorn[standard]>=0.27.0
sqlalchemy>=2.0.27
pydantic>=2.6.1
pydantic-settings>=2.1.0
alembic>=1.13.1
asyncpg>=0.29.0
httpx>=0.26.0
python-jose[cryptography]>=3.3.0
passlib[bcrypt]>=1.7.4
python-multipart>=0.0.9
tenacity>=8.2.3
greenlet>=3.0.3
aiosqlite
litellm
cryptography
databricks
databricks-sdk
croniter
crewai
pydantic[email]
email-validator
google-api-python-client
pysendpulse
langchain
crewai_tools==0.45.0
nixtla
selenium
python-pptx
urllib3>=1.26.6
mcp==1.9.0
mcpadapt
bcrypt==4.0.1
starlette==0.40.0
"""
        with open(requirements_path, "w") as f:
            f.write(requirements_content)
    
    try:
        # Check if app exists, create if not
        try:
            app_exists = False
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
                # Configure OAuth scopes for existing app if requested
                if configure_oauth:
                    logger.info("Updating OAuth scopes for existing app...")
                    oauth_success = configure_oauth_scopes(app_name, exclude_dataplane=exclude_dataplane)
                    if not oauth_success:
                        logger.warning("OAuth scope configuration failed, but continuing with deployment")

        except Exception as e:
            logger.error(f"Error checking/creating app: {e}")
            raise
        
        # Create databricksdist folder with only the files we need
        try:
            databricks_dist = root_dir / "databricksdist"
            logger.info(f"Creating clean databricks deployment directory: {databricks_dist}")
            
            # Remove and recreate databricksdist directory
            if databricks_dist.exists():
                shutil.rmtree(databricks_dist)
            databricks_dist.mkdir()
            
            # Copy backend folder
            logger.info("Copying backend folder...")
            backend_src = root_dir / "backend"
            backend_dst = databricks_dist / "backend"
            if backend_src.exists():
                shutil.copytree(backend_src, backend_dst, ignore=shutil.ignore_patterns(
                    '__pycache__', '*.pyc', '*.pyo', 'logs', '*.log',
                    '.mypy_cache', '.pytest_cache', 'htmlcov', 'tests',
                    '*.db', '*.db-shm', '*.db-wal', '*.backup', '.coverage',
                    '.venv', 'venv', '.env', 'node_modules', '.git'
                ))
                logger.info(f"Copied backend folder")
            else:
                logger.error("Backend folder not found!")
                raise FileNotFoundError("Backend folder not found")
            
            # Copy frontend source folder (without node_modules, dist, coverage)
            logger.info("Copying frontend source folder...")
            frontend_src = root_dir / "frontend"
            frontend_dst = databricks_dist / "frontend"
            if frontend_src.exists():
                shutil.copytree(frontend_src, frontend_dst, ignore=shutil.ignore_patterns(
                    'node_modules', 'dist', 'coverage', '.env.local',
                    '.env.development.local', '.env.test.local', '.env.production.local'
                ))
                logger.info("Copied frontend source folder")
            else:
                logger.error("Frontend source folder not found!")
                raise FileNotFoundError("frontend/ folder not found")

            # Copy docs folder (markdown files for frontend)
            logger.info("Copying docs folder...")
            docs_src = root_dir / "docs"
            docs_dst = databricks_dist / "docs"
            if docs_src.exists():
                shutil.copytree(docs_src, docs_dst, ignore=shutil.ignore_patterns(
                    'archive', '*.pyc', '__pycache__'
                ))
                logger.info("Copied docs folder")
            else:
                logger.warning("docs/ folder not found, skipping")

            # Copy essential files (including package.json for frontend build)
            essential_files = ["app.yaml", "requirements.txt", "entrypoint.py", "package.json"]
            for file_name in essential_files:
                src_file = root_dir / file_name
                dst_file = databricks_dist / file_name
                if src_file.exists():
                    shutil.copy2(src_file, dst_file)
                    logger.info(f"Copied {file_name}")
                else:
                    logger.warning(f"{file_name} not found, skipping")
            

            # Hybrid upload strategy:
            # 1. Use import-dir for bulk directory uploads (backend/, frontend/, docs/)
            # 2. Use SDK workspace.upload() for root-level files to avoid .py→notebook conversion
            logger.info(f"Uploading deployment to workspace: {workspace_dir}")
            logger.info(f"Uploading from: {databricks_dist}")
            logger.info(f"Contents: backend/, frontend/, docs/, package.json, app.yaml, requirements.txt, entrypoint.py")
            confirmation = input("Do you want to proceed with this upload? (y/N): ")

            if confirmation.lower() not in ['y', 'yes']:
                logger.info("Upload cancelled by user")
                return False

            logger.info("Proceeding with upload...")

            # Step 1: Upload directories using import-dir (safe for non-.py bulk content)
            directories_to_upload = ["backend", "frontend", "docs"]
            for dir_name in directories_to_upload:
                dir_path = databricks_dist / dir_name
                if dir_path.exists():
                    target_path = f"{workspace_dir}/{dir_name}"
                    import_cmd = [
                        "databricks", "workspace", "import-dir",
                        "--overwrite",
                        str(dir_path),
                        target_path
                    ]
                    logger.info(f"Uploading {dir_name}/ to {target_path}")
                    result = subprocess.run(import_cmd, check=True, capture_output=True, text=True)
                    logger.info(f"Uploaded {dir_name}/ successfully")
                    if result.stderr:
                        logger.warning(f"Upload warnings for {dir_name}/: {result.stderr}")
                else:
                    logger.warning(f"Directory {dir_name}/ not found in databricksdist, skipping")

            # Step 2: Upload root-level files individually using SDK
            # This avoids the import-dir bug that converts .py files to notebooks
            root_files = ["entrypoint.py", "requirements.txt", "app.yaml", "package.json"]
            for file_name in root_files:
                file_path = databricks_dist / file_name
                if file_path.exists():
                    target_path = f"{workspace_dir}/{file_name}"
                    logger.info(f"Uploading {file_name} to {target_path}")
                    with open(file_path, "rb") as f:
                        client.workspace.upload(
                            target_path,
                            f,
                            format=ImportFormat.AUTO,
                            overwrite=True
                        )
                    logger.info(f"Uploaded {file_name} successfully")
                else:
                    logger.warning(f"File {file_name} not found in databricksdist, skipping")

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
            
            # Deploy the app
            try:
                logger.info("Deploying application")
                waiter = client.apps.deploy(
                    app_name=app_name,
                    app_deployment=app_deployment
                )
                result = waiter.result()
                deployment_id = result.deployment_id
                logger.info(f"Deployment created with ID: {deployment_id}")
            except Exception as e1:
                logger.error(f"Deployment attempt failed with error type: {type(e1)}")
                logger.error(f"Deployment error: {e1}")
                
                try:
                    # Try with minimal parameters
                    logger.info("Attempt 2: Using minimal parameters")
                    result = client.apps.deploy(
                        app_name=app_name,
                        app_deployment=app_deployment
                    )
                    deployment_id = result.deployment_id
                    logger.info(f"Second attempt succeeded with ID: {deployment_id}")
                except Exception as e2:
                    logger.error(f"Second deployment attempt failed with error type: {type(e2)}")
                    logger.error(f"Second deployment error: {e2}")
                    logger.error("All deployment attempts failed")
                    return False
            
            # Wait a bit for deployment to complete and then start the app
            logger.info("Waiting for deployment to complete...")
            time.sleep(10)
            
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

    args = parser.parse_args()
    
    # Validate app name (lowercase letters, numbers, and hyphens only)
    import re
    if not re.match(r'^[a-z0-9-]+$', args.app_name):
        logger.error("App name must contain only lowercase letters, numbers, and hyphens")
        sys.exit(1)
    
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
            exclude_dataplane=not args.include_dataplane
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
