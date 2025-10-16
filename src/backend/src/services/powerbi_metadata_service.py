"""
Power BI Metadata Service

This service manages automatic metadata extraction and caching for Power BI semantic models.
It integrates with the metadata extractor notebook to fetch table/column information on-demand.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

logger = logging.getLogger(__name__)


class PowerBIMetadataService:
    """
    Service for automatic Power BI metadata extraction and caching.

    This service:
    1. Runs the metadata extractor notebook when needed
    2. Caches metadata results to avoid repeated extractions
    3. Provides metadata to DAXGeneratorService automatically
    """

    def __init__(self, session: AsyncSession, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize the Power BI Metadata Service.

        Args:
            session: SQLAlchemy async session for database operations
            workspace_client: Optional Databricks workspace client
        """
        self.session = session
        self.workspace_client = workspace_client or WorkspaceClient()
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_expiry: Dict[str, datetime] = {}
        self.cache_ttl_hours = 24  # Cache metadata for 24 hours

    @classmethod
    async def from_unit_of_work(cls, uow, workspace_client: Optional[WorkspaceClient] = None):
        """
        Create service instance from Unit of Work.

        Args:
            uow: Unit of Work instance
            workspace_client: Optional Databricks workspace client

        Returns:
            PowerBIMetadataService instance
        """
        return cls(uow.session, workspace_client)

    async def get_metadata(
        self,
        semantic_model_id: str,
        workspace_id: str,
        auth_config: Dict[str, str],
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Get metadata for a Power BI semantic model.

        This method:
        1. Checks cache first
        2. If not cached or expired, runs metadata extractor notebook
        3. Caches result for future use

        Args:
            semantic_model_id: Power BI semantic model/dataset ID
            workspace_id: Power BI workspace ID
            auth_config: Authentication configuration containing:
                - auth_method: "device_code" or "service_principal"
                - tenant_id: Azure AD tenant ID (optional, has default)
                - client_id: Azure AD client ID (optional, has default)
                - client_secret: Service principal secret (if using service_principal)
            force_refresh: Force metadata refresh even if cached

        Returns:
            Dictionary containing metadata structure:
            {
                "tables": [
                    {
                        "name": "TableName",
                        "columns": [
                            {"name": "ColumnName", "data_type": "string|int|decimal|datetime"}
                        ]
                    }
                ]
            }
        """
        cache_key = f"{workspace_id}:{semantic_model_id}"

        # Check cache first (unless force refresh)
        if not force_refresh and self._is_cached(cache_key):
            logger.info(f"Using cached metadata for semantic model: {semantic_model_id}")
            return self._metadata_cache[cache_key]

        # Extract metadata by running notebook
        logger.info(f"Extracting metadata for semantic model: {semantic_model_id}")

        try:
            metadata = await self._extract_metadata_via_notebook(
                semantic_model_id=semantic_model_id,
                workspace_id=workspace_id,
                auth_config=auth_config
            )

            # Cache the result
            self._metadata_cache[cache_key] = metadata
            self._cache_expiry[cache_key] = datetime.now() + timedelta(hours=self.cache_ttl_hours)

            logger.info(
                f"Metadata extracted and cached for {semantic_model_id}: "
                f"{len(metadata.get('tables', []))} tables"
            )

            return metadata

        except Exception as e:
            logger.error(f"Error extracting metadata: {str(e)}", exc_info=True)
            raise

    async def _extract_metadata_via_notebook(
        self,
        semantic_model_id: str,
        workspace_id: str,
        auth_config: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Extract metadata by running the powerbi_metadata_extractor notebook.

        Args:
            semantic_model_id: Power BI semantic model ID
            workspace_id: Power BI workspace ID
            auth_config: Authentication configuration

        Returns:
            Extracted metadata dictionary
        """
        try:
            # Get authenticated user's email for notebook path
            current_user = self.workspace_client.current_user.me()
            user_email = current_user.user_name

            # Notebook path
            notebook_path = f"/Users/{user_email}/kasal_notebooks/powerbi_metadata_extractor"

            # Build job parameters
            job_params = {
                "workspace_id": workspace_id,
                "semantic_model_id": semantic_model_id,
                "auth_method": auth_config.get("auth_method", "service_principal"),
                "sample_size": 100,
                "output_format": "json"
            }

            # Add auth-specific parameters
            if auth_config.get("tenant_id"):
                job_params["tenant_id"] = auth_config["tenant_id"]
            if auth_config.get("client_id"):
                job_params["client_id"] = auth_config["client_id"]
            if auth_config.get("client_secret"):
                job_params["client_secret"] = auth_config["client_secret"]

            logger.info(f"Running metadata extractor notebook: {notebook_path}")
            logger.info(f"Job parameters: {self._sanitize_params_for_log(job_params)}")

            # Submit notebook run
            run_result = self.workspace_client.jobs.submit(
                run_name=f"powerbi_metadata_extraction_{semantic_model_id}",
                tasks=[
                    jobs.SubmitTask(
                        task_key="extract_metadata",
                        notebook_task=jobs.NotebookTask(
                            notebook_path=notebook_path,
                            base_parameters={"job_params": json.dumps(job_params)}
                        ),
                        # Use a small cluster for metadata extraction
                        new_cluster=jobs.ClusterSpec(
                            spark_version="13.3.x-scala2.12",
                            node_type_id="i3.xlarge",
                            num_workers=0,  # Single node cluster
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
            logger.info(f"Metadata extraction job submitted: run_id={run_id}")

            # Wait for completion
            run = self.workspace_client.jobs.wait_get_run_job_terminated_or_skipped(run_id)

            # Check result
            if run.state.result_state == jobs.RunResultState.SUCCESS:
                logger.info(f"Metadata extraction completed successfully: run_id={run_id}")

                # Get the output from notebook
                output = self.workspace_client.jobs.get_run_output(run_id)

                if output.notebook_output and output.notebook_output.result:
                    result_data = json.loads(output.notebook_output.result)

                    # Extract the compact_metadata (PowerBITool format)
                    if "compact_metadata" in result_data:
                        return result_data["compact_metadata"]
                    elif "metadata" in result_data:
                        return result_data["metadata"]
                    else:
                        raise ValueError("Notebook output missing metadata")

                else:
                    raise ValueError("No output from metadata extractor notebook")

            else:
                error_msg = f"Metadata extraction failed: {run.state.state_message}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        except Exception as e:
            logger.error(f"Error running metadata extraction notebook: {str(e)}", exc_info=True)
            raise

    def _is_cached(self, cache_key: str) -> bool:
        """
        Check if metadata is cached and not expired.

        Args:
            cache_key: Cache key for the metadata

        Returns:
            True if cached and valid, False otherwise
        """
        if cache_key not in self._metadata_cache:
            return False

        if cache_key not in self._cache_expiry:
            return False

        # Check if expired
        if datetime.now() > self._cache_expiry[cache_key]:
            # Remove expired entry
            del self._metadata_cache[cache_key]
            del self._cache_expiry[cache_key]
            return False

        return True

    def clear_cache(self, semantic_model_id: Optional[str] = None, workspace_id: Optional[str] = None):
        """
        Clear metadata cache.

        Args:
            semantic_model_id: If provided, clear only this model's cache
            workspace_id: If provided along with semantic_model_id, clear specific cache
        """
        if semantic_model_id and workspace_id:
            cache_key = f"{workspace_id}:{semantic_model_id}"
            if cache_key in self._metadata_cache:
                del self._metadata_cache[cache_key]
                del self._cache_expiry[cache_key]
                logger.info(f"Cleared cache for: {cache_key}")
        else:
            self._metadata_cache.clear()
            self._cache_expiry.clear()
            logger.info("Cleared all metadata cache")

    def get_cache_status(self) -> Dict[str, Any]:
        """
        Get current cache status.

        Returns:
            Dictionary with cache statistics
        """
        return {
            "cached_models": len(self._metadata_cache),
            "cache_keys": list(self._metadata_cache.keys()),
            "cache_ttl_hours": self.cache_ttl_hours
        }

    def _sanitize_params_for_log(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize parameters for logging (hide secrets).

        Args:
            params: Original parameters

        Returns:
            Sanitized parameters with secrets masked
        """
        sanitized = params.copy()
        if "client_secret" in sanitized:
            sanitized["client_secret"] = "***REDACTED***"
        return sanitized
