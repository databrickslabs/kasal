"""
Notebook Deployment Utility

Handles deployment of notebook templates to Databricks workspace.
"""

import logging
from pathlib import Path
from typing import Optional, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)


class NotebookDeployer:
    """Deploy notebook templates to Databricks workspace."""

    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize NotebookDeployer.

        Args:
            workspace_client: Optional WorkspaceClient instance. If not provided,
                             will create one using default authentication.
        """
        self.client = workspace_client or WorkspaceClient()

    def deploy_notebook(
        self,
        notebook_path: Path,
        workspace_path: str,
        overwrite: bool = True,
        format: ImportFormat = ImportFormat.SOURCE
    ) -> bool:
        """
        Deploy a single notebook to Databricks workspace.

        Args:
            notebook_path: Local path to the notebook file
            workspace_path: Destination path in Databricks workspace
            overwrite: Whether to overwrite existing notebook
            format: Import format (SOURCE, HTML, JUPYTER, or DBC)

        Returns:
            bool: True if deployment successful, False otherwise
        """
        try:
            logger.info(f"Deploying notebook: {notebook_path.name}")
            logger.info(f"  Source: {notebook_path}")
            logger.info(f"  Destination: {workspace_path}")

            # Read notebook content
            with open(notebook_path, 'rb') as f:
                content = f.read()

            # Import notebook to workspace
            self.client.workspace.import_(
                path=workspace_path,
                format=format,
                content=content,
                overwrite=overwrite
            )

            logger.info(f"✅ Notebook deployed successfully: {workspace_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to deploy notebook {notebook_path.name}: {str(e)}")
            return False

    def deploy_notebooks_directory(
        self,
        notebooks_dir: Path,
        workspace_base_path: str,
        overwrite: bool = True,
        patterns: Optional[List[str]] = None
    ) -> dict:
        """
        Deploy all notebooks from a directory to Databricks workspace.

        Args:
            notebooks_dir: Local directory containing notebooks
            workspace_base_path: Base path in Databricks workspace
            overwrite: Whether to overwrite existing notebooks
            patterns: Optional list of file patterns to include (e.g., ['*.py', '*.ipynb'])

        Returns:
            dict: Summary of deployment with success/failure counts
        """
        if not notebooks_dir.exists():
            logger.error(f"Notebooks directory not found: {notebooks_dir}")
            return {"success": 0, "failed": 0, "total": 0}

        # Default patterns if none provided
        if patterns is None:
            patterns = ['*.py', '*.ipynb']

        # Find all matching notebook files
        notebook_files = []
        for pattern in patterns:
            notebook_files.extend(notebooks_dir.glob(pattern))

        if not notebook_files:
            logger.warning(f"No notebook files found in {notebooks_dir}")
            return {"success": 0, "failed": 0, "total": 0}

        logger.info(f"Found {len(notebook_files)} notebooks to deploy")

        # Deploy each notebook
        success_count = 0
        failed_count = 0

        for notebook_file in notebook_files:
            # Determine workspace path
            relative_path = notebook_file.relative_to(notebooks_dir)
            workspace_path = f"{workspace_base_path}/{relative_path}".replace('\\', '/')

            # Remove .py or .ipynb extension from workspace path
            if workspace_path.endswith('.py'):
                workspace_path = workspace_path[:-3]
            elif workspace_path.endswith('.ipynb'):
                workspace_path = workspace_path[:-6]

            # Deploy notebook
            if self.deploy_notebook(notebook_file, workspace_path, overwrite):
                success_count += 1
            else:
                failed_count += 1

        summary = {
            "success": success_count,
            "failed": failed_count,
            "total": len(notebook_files)
        }

        logger.info("=" * 80)
        logger.info("Notebook Deployment Summary")
        logger.info("=" * 80)
        logger.info(f"✅ Successfully deployed: {success_count}")
        logger.info(f"❌ Failed to deploy: {failed_count}")
        logger.info(f"📊 Total notebooks: {len(notebook_files)}")
        logger.info("=" * 80)

        return summary

    def ensure_workspace_directory(self, workspace_path: str) -> bool:
        """
        Ensure a directory exists in Databricks workspace.

        Args:
            workspace_path: Path to directory in workspace

        Returns:
            bool: True if directory exists or was created, False otherwise
        """
        try:
            # Try to get directory info
            try:
                self.client.workspace.get_status(workspace_path)
                logger.debug(f"Directory exists: {workspace_path}")
                return True
            except Exception:
                # Directory doesn't exist, create it
                self.client.workspace.mkdirs(workspace_path)
                logger.info(f"✅ Created workspace directory: {workspace_path}")
                return True

        except Exception as e:
            logger.error(f"❌ Failed to ensure directory {workspace_path}: {str(e)}")
            return False


def deploy_powerbi_notebooks(
    user_name: str,
    workspace_client: Optional[WorkspaceClient] = None,
    overwrite: bool = True
) -> dict:
    """
    Deploy Power BI related notebooks to Databricks workspace.

    This function deploys the Power BI DAX executor notebook template
    to the user's workspace.

    Args:
        user_name: Databricks user name (e.g., user@example.com)
        workspace_client: Optional WorkspaceClient instance
        overwrite: Whether to overwrite existing notebooks

    Returns:
        dict: Deployment summary
    """
    deployer = NotebookDeployer(workspace_client)

    # Determine paths
    templates_dir = Path(__file__).parent.parent / "engines" / "crewai" / "tools" / "templates" / "notebooks"
    workspace_base_path = f"/Users/{user_name}/kasal_notebooks"

    logger.info(f"Deploying Power BI notebooks from: {templates_dir}")
    logger.info(f"Deploying to: {workspace_base_path}")

    # Ensure workspace directory exists
    deployer.ensure_workspace_directory(workspace_base_path)

    # Deploy notebooks
    summary = deployer.deploy_notebooks_directory(
        notebooks_dir=templates_dir,
        workspace_base_path=workspace_base_path,
        overwrite=overwrite,
        patterns=['powerbi_*.py']  # Only deploy Power BI notebooks
    )

    return summary


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python notebook_deployment.py <user_name>")
        print("Example: python notebook_deployment.py user@example.com")
        sys.exit(1)

    user_name = sys.argv[1]

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Deploy notebooks
    summary = deploy_powerbi_notebooks(user_name)

    if summary["failed"] > 0:
        sys.exit(1)
    else:
        sys.exit(0)
