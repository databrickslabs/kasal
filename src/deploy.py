#!/usr/bin/env python3
"""
Direct deployment script for Kasal application.

This script deploys the backend code as-is and uses import-dir for frontend_static.
"""

import os
import shutil
import subprocess
import sys
import logging
import time
import argparse
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
    api_url=None
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
    
    # Check that frontend_static exists
    frontend_static_dir = root_dir / "frontend_static"
    if not frontend_static_dir.exists():
        logger.error("frontend_static directory does not exist. Please run 'python build.py' first to build the frontend.")
        raise FileNotFoundError("frontend_static directory not found")
    
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
                shutil.copytree(backend_src, backend_dst, ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '*.pyo', 'logs', '*.log', '.mypy_cache', '.pytest_cache'))
                logger.info(f"Copied backend folder")
            else:
                logger.error("Backend folder not found!")
                raise FileNotFoundError("Backend folder not found")
            
            # Copy frontend_static folder
            logger.info("Copying frontend_static folder...")
            frontend_static_src = root_dir / "frontend_static"
            frontend_static_dst = databricks_dist / "frontend_static"
            if frontend_static_src.exists():
                shutil.copytree(frontend_static_src, frontend_static_dst)
                logger.info(f"Copied frontend_static folder")
            else:
                logger.error("frontend_static folder not found!")
                raise FileNotFoundError("frontend_static folder not found")
            
            # Copy essential files
            essential_files = ["app.yaml", "requirements.txt", "entrypoint.py"]
            for file_name in essential_files:
                src_file = root_dir / file_name
                dst_file = databricks_dist / file_name
                if src_file.exists():
                    shutil.copy2(src_file, dst_file)
                    logger.info(f"Copied {file_name}")
                else:
                    logger.warning(f"{file_name} not found, skipping")
            
            
            # Upload databricksdist folder using databricks CLI import-dir
            logger.info(f"Uploading clean deployment folder to workspace using import-dir")
            
            import_cmd = [
                "databricks", "workspace", "import-dir", 
                "--overwrite",
                str(databricks_dist), 
                workspace_dir
            ]
            
            logger.info(f"About to run command: {' '.join(import_cmd)}")
            logger.info(f"Uploading from: {databricks_dist}")
            logger.info(f"Contents: backend/, frontend_static/, app.yaml, requirements.txt, entrypoint.py")
            confirmation = input("Do you want to proceed with this command? (y/N): ")
            
            if confirmation.lower() not in ['y', 'yes']:
                logger.info("Upload cancelled by user")
                return False
            
            logger.info("Proceeding with upload...")
            result = subprocess.run(import_cmd, check=True, capture_output=True, text=True)
            
            logger.info("Source code uploaded successfully using import-dir")
            logger.info(f"Upload output: {result.stdout}")
            if result.stderr:
                logger.warning(f"Upload warnings: {result.stderr}")
            
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
            api_url=args.api_url
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