#!/usr/bin/env python3

import os
import sys
import argparse
import logging
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

def deploy_wheel_to_databricks(
    wheel_path, 
    app_name="kasal",
    user_name=None,
    workspace_dir=None,
    profile=None, 
    host=None, 
    token=None, 
    description=None
):
    """Deploy a wheel file to Databricks Apps"""
    wheel_path = Path(wheel_path)
    if not wheel_path.exists():
        raise FileNotFoundError(f"Wheel file not found: {wheel_path}")
    
    logger.info(f"Deploying wheel file: {wheel_path}")
    
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
    
    # Create app.yaml in the dist folder
    dist_dir = os.path.dirname(wheel_path)
    app_yaml_path = os.path.join(dist_dir, "app.yaml")
    logger.info(f"Creating app.yaml at {app_yaml_path}")
    with open(app_yaml_path, "w") as f:
        f.write("command: ['python', '-m', 'kasal']\n")
        f.write("environment_vars:\n")
        f.write("  PYTHONPATH: '.:${PYTHONPATH}'\n")
        f.write("  PYTHONUNBUFFERED: '1'\n")  # Ensures logs appear immediately
        f.write("apt_packages:\n")
        f.write("  - libpq-dev\n")  # PostgreSQL development headers for psycopg2
    
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
        
        # Create workspace directory and upload files
        try:
            # Ensure the workspace directory exists
            logger.info(f"Creating workspace directory: {workspace_dir}")
            client.workspace.mkdirs(path=workspace_dir)
            
            # Upload app.yaml file
            logger.info("Uploading app.yaml file")
            yaml_remote_path = f"{workspace_dir}/app.yaml"
            with open(app_yaml_path, "rb") as f:
                yaml_content = f.read()
                client.workspace.upload(
                    path=yaml_remote_path,
                    content=yaml_content,
                    overwrite=True,
                    format=ImportFormat.AUTO
                )
            
            # Upload requirements.txt file if it exists
            requirements_path = os.path.join(dist_dir, "requirements.txt")
            if os.path.exists(requirements_path):
                logger.info(f"Uploading requirements.txt file")
                req_remote_path = f"{workspace_dir}/requirements.txt"
                with open(requirements_path, "rb") as f:
                    req_content = f.read()
                    client.workspace.upload(
                        path=req_remote_path,
                        content=req_content,
                        overwrite=True,
                        format=ImportFormat.AUTO
                    )
            else:
                # Create a requirements.txt file that installs the wheel
                logger.info("Creating requirements.txt file with wheel installation")
                wheel_install = f"./{wheel_path.name}\n"
                req_remote_path = f"{workspace_dir}/requirements.txt"
                client.workspace.upload(
                    path=req_remote_path,
                    content=wheel_install.encode(),
                    overwrite=True,
                    format=ImportFormat.AUTO
                )
            
            # Upload the wheel file
            logger.info(f"Uploading wheel file: {wheel_path.name}")
            wheel_remote_path = f"{workspace_dir}/{wheel_path.name}"
            with open(wheel_path, "rb") as f:
                wheel_content = f.read()
                client.workspace.upload(
                    path=wheel_remote_path,
                    content=wheel_content,
                    overwrite=True,
                    format=ImportFormat.AUTO
                )
            
            logger.info("All files uploaded successfully")
        except Exception as e:
            logger.error(f"Error uploading files: {e}")
            raise
        
        # Deploy using absolute minimum code with enhanced logging
        try:
            logger.info(f"Deploying app {app_name} from {workspace_dir}")
            
            # Log all imported classes and their attributes
            logger.info(f"AppDeploymentMode type: {type(AppDeploymentMode)}")
            logger.info(f"AppDeploymentMode class dir: {dir(AppDeploymentMode)}")
            logger.info(f"AppDeploymentMode values: {[mode for mode in AppDeploymentMode]}")
            
            # Create an AppDeployment object to use with deploy
            try:
                logger.info(f"Creating AppDeployment object with workspace_dir={workspace_dir}")
                app_deployment = AppDeployment(
                    source_code_path=workspace_dir,
                    mode=AppDeploymentMode.SNAPSHOT  # Using SNAPSHOT mode since WORKSPACE is not available
                )
                logger.info(f"AppDeployment object created successfully")
                logger.info(f"AppDeployment object type: {type(app_deployment)}")
                logger.info(f"AppDeployment object dir: {dir(app_deployment)}")
                logger.info(f"AppDeployment object __dict__: {app_deployment.__dict__}")
            except Exception as e:
                logger.error(f"Error creating AppDeployment object: {e}")
                raise
                
            # Log the deploy method signature
            import inspect
            logger.info(f"apps.deploy method signature: {inspect.signature(client.apps.deploy)}")
            
            # Try with different formats based on SDK version 0.53.0
            try:
                # Format for SDK version 0.53.0
                logger.info("Using correct parameters for SDK version 0.53.0")
                # Pass app_deployment as a positional argument
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
                    # Create a new deployment with the app name and app_deployment
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
                # Try to get more details from the app's status
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
            # Try to dump the full error details
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
    except Exception as e:
        logger.error(f"Error during deployment: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Deploy a wheel file to Databricks Apps")
    parser.add_argument("--wheel", default="dist/kasal-0.1.0-py3-none-any.whl", 
                        help="Path to the wheel file to deploy")
    parser.add_argument("--app-name", default="kasal", required=True,
                        help="Name for the Databricks App (lowercase with hyphens only)")
    parser.add_argument("--user-name", default="nehme.tohme@databricks.com", required=True,
                        help="User name for workspace path")
    parser.add_argument("--workspace-dir", 
                        help="Workspace directory to upload files (default: /Workspace/Users/<user-name>/<app-name>)")
    parser.add_argument("--profile", help="Databricks CLI profile to use")
    parser.add_argument("--host", help="Databricks host URL")
    parser.add_argument("--token", help="Databricks API token")
    parser.add_argument("--description", help="Description for the app")
    
    args = parser.parse_args()
    
    # Validate app name (lowercase letters, numbers, and hyphens only)
    import re
    if not re.match(r'^[a-z0-9-]+$', args.app_name):
        logger.error("App name must contain only lowercase letters, numbers, and hyphens")
        sys.exit(1)
    
    try:
        success = deploy_wheel_to_databricks(
            wheel_path=args.wheel,
            app_name=args.app_name,
            user_name=args.user_name,
            workspace_dir=args.workspace_dir,
            profile=args.profile,
            host=args.host,
            token=args.token,
            description=args.description
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