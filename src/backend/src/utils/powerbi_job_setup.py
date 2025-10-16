"""
Power BI Job Setup Utility

Uploads Power BI notebooks to Databricks and creates persistent jobs.
Similar to gmaps_search job setup pattern.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service import jobs

logger = logging.getLogger(__name__)


class PowerBIJobSetup:
    """Setup Power BI notebooks and jobs in Databricks."""

    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize the job setup utility.

        Args:
            workspace_client: Optional Databricks workspace client
        """
        self.client = workspace_client or WorkspaceClient()

        # Get notebooks directory
        current_file = Path(__file__)
        self.notebooks_dir = (
            current_file.parent.parent / "engines" / "crewai" / "tools" / "templates" / "notebooks"
        )

    def upload_notebook(
        self,
        notebook_name: str,
        user_email: Optional[str] = None
    ) -> str:
        """
        Upload a notebook to Databricks workspace.

        Args:
            notebook_name: Name of the notebook (without .py extension)
            user_email: Optional user email for workspace path

        Returns:
            Workspace path where notebook was uploaded
        """
        # Determine workspace path
        if user_email:
            workspace_path = f"/Workspace/Users/{user_email}/notebooks/{notebook_name}"
        else:
            current_user = self.client.current_user.me()
            workspace_path = f"/Workspace/Users/{current_user.user_name}/notebooks/{notebook_name}"

        # Local notebook path
        local_notebook_path = self.notebooks_dir / f"{notebook_name}.py"

        if not local_notebook_path.exists():
            raise FileNotFoundError(f"Notebook not found: {local_notebook_path}")

        logger.info(f"Uploading notebook from: {local_notebook_path}")
        logger.info(f"To workspace path: {workspace_path}")

        # Read notebook content
        with open(local_notebook_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Ensure parent directory exists
        parent_path = str(Path(workspace_path).parent)
        try:
            self.client.workspace.mkdirs(parent_path)
        except Exception as e:
            logger.warning(f"Could not create parent directory (may already exist): {e}")

        # Upload notebook
        self.client.workspace.upload(
            path=workspace_path,
            content=content.encode('utf-8'),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )

        logger.info(f"✅ Uploaded notebook to {workspace_path}")

        return workspace_path

    def create_metadata_extractor_job(
        self,
        user_email: Optional[str] = None,
        use_serverless: bool = False
    ) -> int:
        """
        Create a persistent job for Power BI metadata extraction.

        Args:
            user_email: Optional user email for workspace path
            use_serverless: Whether to use serverless compute

        Returns:
            Job ID of created job
        """
        notebook_name = "powerbi_metadata_extractor"

        # Upload notebook
        workspace_path = self.upload_notebook(notebook_name, user_email)

        # Job configuration
        job_name = f"powerbi_metadata_extractor_{user_email or 'default'}"

        # Create task
        if use_serverless:
            # Serverless compute
            task = jobs.Task(
                task_key="extract_metadata",
                notebook_task=jobs.NotebookTask(
                    notebook_path=workspace_path,
                    base_parameters={}
                ),
                timeout_seconds=3600,
                environment_key="Default"  # Serverless environment
            )
        else:
            # Single-node cluster
            task = jobs.Task(
                task_key="extract_metadata",
                notebook_task=jobs.NotebookTask(
                    notebook_path=workspace_path,
                    base_parameters={}
                ),
                timeout_seconds=3600,
                new_cluster=jobs.ClusterSpec(
                    spark_version="13.3.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=0,
                    spark_conf={
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]"
                    },
                    custom_tags={"ResourceClass": "SingleNode"}
                )
            )

        # Create job
        response = self.client.jobs.create(
            name=job_name,
            description="Extract metadata from Power BI semantic models",
            format=jobs.Format.MULTI_TASK,
            tasks=[task],
            max_concurrent_runs=3,
            timeout_seconds=3600
        )

        logger.info(f"✅ Created metadata extractor job with ID: {response.job_id}")

        return response.job_id

    def create_dax_executor_job(
        self,
        user_email: Optional[str] = None,
        use_serverless: bool = False
    ) -> int:
        """
        Create a persistent job for Power BI DAX query execution.

        Args:
            user_email: Optional user email for workspace path
            use_serverless: Whether to use serverless compute

        Returns:
            Job ID of created job
        """
        notebook_name = "powerbi_dax_executor"

        # Upload notebook
        workspace_path = self.upload_notebook(notebook_name, user_email)

        # Job configuration
        job_name = f"powerbi_dax_executor_{user_email or 'default'}"

        # Create task
        if use_serverless:
            # Serverless compute
            task = jobs.Task(
                task_key="execute_dax",
                notebook_task=jobs.NotebookTask(
                    notebook_path=workspace_path,
                    base_parameters={}
                ),
                timeout_seconds=3600,
                environment_key="Default"  # Serverless environment
            )
        else:
            # Single-node cluster
            task = jobs.Task(
                task_key="execute_dax",
                notebook_task=jobs.NotebookTask(
                    notebook_path=workspace_path,
                    base_parameters={}
                ),
                timeout_seconds=3600,
                new_cluster=jobs.ClusterSpec(
                    spark_version="13.3.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=0,
                    spark_conf={
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]"
                    },
                    custom_tags={"ResourceClass": "SingleNode"}
                )
            )

        # Create job
        response = self.client.jobs.create(
            name=job_name,
            description="Execute DAX queries against Power BI datasets",
            format=jobs.Format.MULTI_TASK,
            tasks=[task],
            max_concurrent_runs=5,
            timeout_seconds=3600
        )

        logger.info(f"✅ Created DAX executor job with ID: {response.job_id}")

        return response.job_id

    def setup_all_jobs(
        self,
        user_email: Optional[str] = None,
        use_serverless: bool = False
    ) -> Dict[str, int]:
        """
        Setup all Power BI jobs.

        Args:
            user_email: Optional user email for workspace path
            use_serverless: Whether to use serverless compute

        Returns:
            Dictionary with job IDs: {"metadata_extractor_job_id": ..., "dax_executor_job_id": ...}
        """
        logger.info("Setting up Power BI jobs...")

        metadata_job_id = self.create_metadata_extractor_job(user_email, use_serverless)
        dax_job_id = self.create_dax_executor_job(user_email, use_serverless)

        result = {
            "metadata_extractor_job_id": metadata_job_id,
            "dax_executor_job_id": dax_job_id
        }

        logger.info("✅ All Power BI jobs created successfully!")
        logger.info(f"Metadata Extractor Job ID: {metadata_job_id}")
        logger.info(f"DAX Executor Job ID: {dax_job_id}")

        return result


def setup_powerbi_jobs(
    user_email: Optional[str] = None,
    use_serverless: bool = False
) -> Dict[str, int]:
    """
    Convenience function to setup Power BI jobs.

    Args:
        user_email: Optional user email for workspace path
        use_serverless: Whether to use serverless compute

    Returns:
        Dictionary with job IDs
    """
    setup = PowerBIJobSetup()
    return setup.setup_all_jobs(user_email, use_serverless)


# CLI usage
if __name__ == "__main__":
    import sys

    user_email = sys.argv[1] if len(sys.argv) > 1 else None
    use_serverless = "--serverless" in sys.argv

    print(f"Setting up Power BI jobs for user: {user_email or 'current user'}")
    print(f"Using serverless: {use_serverless}")

    result = setup_powerbi_jobs(user_email, use_serverless)

    print("\n" + "="*60)
    print("✅ Setup Complete!")
    print("="*60)
    print(f"Metadata Extractor Job ID: {result['metadata_extractor_job_id']}")
    print(f"DAX Executor Job ID: {result['dax_executor_job_id']}")
    print("\nSave these job IDs - you'll need them for your crew configuration!")
    print("="*60)
