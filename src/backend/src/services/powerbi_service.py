"""
Power BI Integration Service

This service provides end-to-end Power BI integration:
1. Automatic metadata extraction
2. DAX query generation from natural language
3. DAX query execution via Databricks notebooks

Users only need to provide:
- semantic_model_id
- question

Everything else is handled automatically!
"""

import json
import logging
from typing import Any, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from src.services.powerbi_metadata_service import PowerBIMetadataService
from src.services.dax_generator_service import DAXGeneratorService

logger = logging.getLogger(__name__)


class PowerBIService:
    """
    End-to-end Power BI integration service.

    This service orchestrates:
    1. Metadata extraction (automatic, cached)
    2. DAX query generation from questions
    3. DAX query execution in Databricks

    Usage:
        service = await PowerBIService.from_unit_of_work(uow)

        # User only provides semantic_model_id and question!
        result = await service.generate_and_prepare_dax(
            semantic_model_id="a17de62e-8dc0-4a8a-acaa-2a9954de8c75",
            workspace_id="bcb084ed-f8c9-422c-b148-29839c0f9227",
            question="What is the total NSR per product?",
            auth_config={
                "auth_method": "service_principal",
                "client_id": "...",
                "tenant_id": "...",
                "client_secret": "..."
            }
        )

        # Result contains DAX query ready to execute!
    """

    def __init__(
        self,
        session: AsyncSession,
        workspace_client: Optional[WorkspaceClient] = None
    ):
        """
        Initialize the Power BI Service.

        Args:
            session: SQLAlchemy async session
            workspace_client: Optional Databricks workspace client
        """
        self.session = session
        self.workspace_client = workspace_client or WorkspaceClient()

        # Initialize sub-services
        self.metadata_service = PowerBIMetadataService(session, workspace_client)
        self.dax_generator_service = DAXGeneratorService(session)

    @classmethod
    async def from_unit_of_work(cls, uow, workspace_client: Optional[WorkspaceClient] = None):
        """
        Create service instance from Unit of Work.

        Args:
            uow: Unit of Work instance
            workspace_client: Optional Databricks workspace client

        Returns:
            PowerBIService instance
        """
        return cls(uow.session, workspace_client)

    async def generate_and_prepare_dax(
        self,
        semantic_model_id: str,
        workspace_id: str,
        question: str,
        auth_config: Dict[str, str],
        model_name: str = "databricks-meta-llama-3-1-405b-instruct",
        temperature: float = 0.1,
        force_metadata_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Generate DAX query from question with automatic metadata extraction.

        This is the main method users call - it handles everything automatically!

        Args:
            semantic_model_id: Power BI semantic model/dataset ID
            workspace_id: Power BI workspace ID
            question: Natural language question (e.g., "What is total sales per region?")
            auth_config: Authentication configuration:
                {
                    "auth_method": "service_principal",
                    "client_id": "...",
                    "tenant_id": "...",
                    "client_secret": "..."
                }
            model_name: LLM model to use for DAX generation
            temperature: Temperature for LLM generation
            force_metadata_refresh: Force metadata refresh even if cached

        Returns:
            Dictionary containing:
            {
                "dax_query": "EVALUATE ...",
                "explanation": "This query calculates...",
                "confidence": 0.9,
                "metadata": {...},  # The metadata used
                "ready_for_execution": True,
                "execution_params": {
                    "semantic_model_id": "...",
                    "workspace_id": "...",
                    "dax_statement": "..."
                }
            }
        """
        try:
            logger.info(f"PowerBI Service: Generating DAX for question: {question[:100]}")
            logger.info(f"  Semantic Model ID: {semantic_model_id}")
            logger.info(f"  Workspace ID: {workspace_id}")

            # Step 1: Get metadata (automatic extraction with caching)
            logger.info("Step 1: Extracting metadata...")
            metadata = await self.metadata_service.get_metadata(
                semantic_model_id=semantic_model_id,
                workspace_id=workspace_id,
                auth_config=auth_config,
                force_refresh=force_metadata_refresh
            )

            logger.info(f"  Metadata retrieved: {len(metadata.get('tables', []))} tables")

            # Step 2: Generate DAX query
            logger.info("Step 2: Generating DAX query...")
            dax_result = await self.dax_generator_service.generate_dax_from_question(
                question=question,
                metadata=metadata,
                model_name=model_name,
                temperature=temperature
            )

            logger.info(f"  DAX generated with confidence: {dax_result['confidence']:.0%}")

            # Step 3: Prepare execution parameters
            execution_params = {
                "semantic_model_id": semantic_model_id,
                "workspace_id": workspace_id,
                "dax_statement": dax_result["dax_query"],
                "auth_method": auth_config.get("auth_method", "service_principal")
            }

            # Add auth parameters
            if auth_config.get("client_id"):
                execution_params["client_id"] = auth_config["client_id"]
            if auth_config.get("tenant_id"):
                execution_params["tenant_id"] = auth_config["tenant_id"]
            if auth_config.get("client_secret"):
                execution_params["client_secret"] = auth_config["client_secret"]

            # Build complete result
            result = {
                "dax_query": dax_result["dax_query"],
                "explanation": dax_result["explanation"],
                "confidence": dax_result["confidence"],
                "question": question,
                "metadata": metadata,
                "metadata_tables_count": len(metadata.get("tables", [])),
                "ready_for_execution": True,
                "execution_params": execution_params,
                "next_step": (
                    "Use DatabricksJobsTool to execute this DAX query. "
                    "Pass the execution_params as job_params to the powerbi_dax_executor notebook."
                )
            }

            logger.info("✅ PowerBI Service: DAX generation complete and ready for execution")

            return result

        except Exception as e:
            logger.error(f"Error in PowerBI Service: {str(e)}", exc_info=True)
            raise

    async def execute_dax_query(
        self,
        dax_query: str,
        semantic_model_id: str,
        workspace_id: str,
        auth_config: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Execute a DAX query via the powerbi_dax_executor notebook.

        Args:
            dax_query: DAX query to execute
            semantic_model_id: Power BI semantic model ID
            workspace_id: Power BI workspace ID
            auth_config: Authentication configuration

        Returns:
            Query execution results
        """
        try:
            # Get authenticated user's email for notebook path
            current_user = self.workspace_client.current_user.me()
            user_email = current_user.user_name

            # Notebook path
            notebook_path = f"/Users/{user_email}/kasal_notebooks/powerbi_dax_executor"

            # Build job parameters
            job_params = {
                "dax_statement": dax_query,
                "workspace_id": workspace_id,
                "semantic_model_id": semantic_model_id,
                "auth_method": auth_config.get("auth_method", "service_principal")
            }

            # Add auth parameters
            if auth_config.get("tenant_id"):
                job_params["tenant_id"] = auth_config["tenant_id"]
            if auth_config.get("client_id"):
                job_params["client_id"] = auth_config["client_id"]
            if auth_config.get("client_secret"):
                job_params["client_secret"] = auth_config["client_secret"]

            logger.info(f"Executing DAX query via notebook: {notebook_path}")

            # Submit notebook run
            run_result = self.workspace_client.jobs.submit(
                run_name=f"powerbi_dax_execution_{semantic_model_id}",
                tasks=[
                    jobs.SubmitTask(
                        task_key="execute_dax",
                        notebook_task=jobs.NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters={"job_params": json.dumps(job_params)}
                        ),
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
                ]
            )

            run_id = run_result.run_id
            logger.info(f"DAX execution job submitted: run_id={run_id}")

            # Wait for completion
            run = self.workspace_client.jobs.wait_get_run_job_terminated_or_skipped(run_id)

            # Check result
            if run.state.result_state == jobs.RunResultState.SUCCESS:
                logger.info(f"DAX execution completed successfully: run_id={run_id}")

                # Get the output
                output = self.workspace_client.jobs.get_run_output(run_id)

                if output.notebook_output and output.notebook_output.result:
                    result_data = json.loads(output.notebook_output.result)
                    return {
                        "status": "success",
                        "run_id": run_id,
                        "result": result_data
                    }
                else:
                    return {
                        "status": "success",
                        "run_id": run_id,
                        "result": None,
                        "message": "Execution successful but no output returned"
                    }

            else:
                error_msg = f"DAX execution failed: {run.state.state_message}"
                logger.error(error_msg)
                return {
                    "status": "failed",
                    "run_id": run_id,
                    "error": error_msg
                }

        except Exception as e:
            logger.error(f"Error executing DAX query: {str(e)}", exc_info=True)
            raise

    async def get_metadata_status(
        self,
        semantic_model_id: Optional[str] = None,
        workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get metadata cache status.

        Args:
            semantic_model_id: Optional semantic model ID to check
            workspace_id: Optional workspace ID

        Returns:
            Cache status information
        """
        status = self.metadata_service.get_cache_status()

        if semantic_model_id and workspace_id:
            cache_key = f"{workspace_id}:{semantic_model_id}"
            status["queried_model_cached"] = cache_key in status["cache_keys"]

        return status

    async def refresh_metadata(
        self,
        semantic_model_id: str,
        workspace_id: str,
        auth_config: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Force refresh metadata for a semantic model.

        Args:
            semantic_model_id: Power BI semantic model ID
            workspace_id: Power BI workspace ID
            auth_config: Authentication configuration

        Returns:
            Refreshed metadata
        """
        logger.info(f"Force refreshing metadata for: {semantic_model_id}")

        metadata = await self.metadata_service.get_metadata(
            semantic_model_id=semantic_model_id,
            workspace_id=workspace_id,
            auth_config=auth_config,
            force_refresh=True
        )

        return {
            "status": "refreshed",
            "semantic_model_id": semantic_model_id,
            "workspace_id": workspace_id,
            "tables_count": len(metadata.get("tables", [])),
            "metadata": metadata
        }
