"""
Power BI Integration Tool

CrewAI custom tool for end-to-end Power BI integration:
1. Submits Power BI full pipeline notebook job (metadata extraction, DAX generation, execution)
2. Monitors job completion with automatic polling
3. Extracts and returns DAX execution results

This tool provides a single-call solution for Power BI queries - users just provide
the question and authentication, and the tool handles everything else automatically.
"""

import asyncio
import logging
import json
import time
import os
import hashlib
from typing import Any, Dict, Optional, Type
from datetime import datetime, timedelta

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import aiohttp

logger = logging.getLogger(__name__)

# Global cache to prevent duplicate job submissions
# Maps request_hash -> (run_id, timestamp, result)
_JOB_SUBMISSION_CACHE = {}


class PowerBIToolSchema(BaseModel):
    """Input schema for Power BI Integration Tool."""

    question: str = Field(
        ...,
        description="Natural language question to answer using Power BI (e.g., 'What is the total NSR per product?')"
    )
    semantic_model_id: str = Field(
        ...,
        description="Power BI semantic model/dataset ID (GUID format)"
    )
    workspace_id: str = Field(
        ...,
        description="Power BI workspace ID (GUID format)"
    )
    tenant_id: str = Field(
        ...,
        description="Azure AD tenant ID for authentication"
    )
    client_id: str = Field(
        ...,
        description="Azure AD service principal client ID"
    )
    client_secret: str = Field(
        ...,
        description="Azure AD service principal client secret"
    )
    auth_method: Optional[str] = Field(
        "service_principal",
        description="Authentication method: 'service_principal' or 'device_code' (default: 'service_principal')"
    )
    databricks_host: Optional[str] = Field(
        None,
        description="Optional Databricks host URL for the notebook to use (e.g., 'https://e2-demo-field-eng.cloud.databricks.com/'). If not provided, uses the tool's configured host."
    )
    databricks_token: Optional[str] = Field(
        None,
        description="Optional Databricks API token for the notebook to use. If not provided, uses the tool's configured token."
    )
    timeout_seconds: Optional[int] = Field(
        600,
        description="Maximum time in seconds to wait for job completion (default: 600 / 10 minutes)"
    )


class PowerBITool(BaseTool):
    """
    Power BI Integration Tool for CrewAI agents.

    This tool provides end-to-end Power BI query execution:
    1. Submits a Databricks job running the powerbi_full_pipeline notebook
    2. Waits for job completion (with automatic polling)
    3. Extracts DAX execution results from the job output

    The tool handles all complexity internally - users just provide:
    - question: Natural language question
    - semantic_model_id, workspace_id: Power BI identifiers
    - tenant_id, client_id, client_secret: Azure AD credentials

    The powerbi_full_pipeline notebook performs:
    - Metadata extraction from Power BI
    - DAX query generation via LLM
    - DAX query execution against Power BI

    All results are automatically extracted and returned.
    """

    name: str = "Power BI Query Executor"
    description: str = (
        "Execute Power BI queries from natural language questions. "
        "This tool handles everything automatically: metadata extraction, DAX generation, and query execution. "
        "Required: question, semantic_model_id, workspace_id, tenant_id, client_id, client_secret. "
        "Optional: auth_method ('service_principal' or 'device_code'), databricks_host, databricks_token, timeout_seconds. "
        "If databricks_host/databricks_token are provided, they override the tool's configuration for the notebook. "
        "IMPORTANT: This tool automatically prevents duplicate submissions - identical requests within 30 minutes return cached results. "
        "Only call this tool ONCE per query. Do not retry or call again. "
        "Returns: DAX query execution results directly."
    )
    args_schema: Type[BaseModel] = PowerBIToolSchema

    # Private attributes for authentication (like DatabricksJobsTool)
    _host: str = PrivateAttr(default=None)
    _token: str = PrivateAttr(default=None)
    _job_id: Optional[int] = PrivateAttr(default=None)  # Power BI pipeline job ID

    def __init__(
        self,
        databricks_host: Optional[str] = None,
        powerbi_job_id: Optional[int] = None,
        tool_config: Optional[dict] = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize the Power BI Integration Tool.

        Args:
            databricks_host: Databricks workspace host URL
            powerbi_job_id: Job ID for the powerbi_full_pipeline notebook
            tool_config: Tool configuration with auth details
            **kwargs: Additional keyword arguments
        """
        super().__init__(**kwargs)

        if tool_config is None:
            tool_config = {}

        # Get configuration from tool_config (same pattern as DatabricksJobsTool)
        if tool_config:
            # Check for token
            if 'DATABRICKS_API_KEY' in tool_config:
                self._token = tool_config['DATABRICKS_API_KEY']
            elif 'token' in tool_config:
                self._token = tool_config['token']

            # Get Power BI job ID
            if 'powerbi_job_id' in tool_config:
                self._job_id = tool_config['powerbi_job_id']
            elif powerbi_job_id:
                self._job_id = powerbi_job_id

            # Handle host configuration
            if databricks_host:
                host = databricks_host
            elif 'DATABRICKS_HOST' in tool_config:
                host = tool_config['DATABRICKS_HOST']
            elif 'databricks_host' in tool_config:
                host = tool_config['databricks_host']
            else:
                host = None

            # Process host if found
            if host:
                if isinstance(host, list) and host:
                    host = host[0]
                if isinstance(host, str):
                    # Strip protocol and trailing slash
                    if host.startswith('https://'):
                        host = host[8:]
                    if host.startswith('http://'):
                        host = host[7:]
                    if host.endswith('/'):
                        host = host[:-1]
                self._host = host

        # Try to get authentication if not set
        if not self._token:
            try:
                # Try API Keys Service
                from src.core.unit_of_work import UnitOfWork
                from src.services.api_keys_service import ApiKeysService

                loop = None
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                async def get_databricks_token():
                    async with UnitOfWork():
                        token = await ApiKeysService.get_provider_api_key("databricks") or \
                               await ApiKeysService.get_provider_api_key("DATABRICKS_API_KEY") or \
                               await ApiKeysService.get_provider_api_key("DATABRICKS_TOKEN")
                        return token

                if not loop.is_running():
                    self._token = loop.run_until_complete(get_databricks_token())

            except Exception as e:
                logger.debug(f"Could not get token from API Keys Service: {e}")

            # Fallback to environment variables
            if not self._token:
                self._token = os.getenv("DATABRICKS_API_KEY") or os.getenv("DATABRICKS_TOKEN")

        # Set fallback host from environment
        if not self._host:
            self._host = os.getenv("DATABRICKS_HOST", "your-workspace.cloud.databricks.com")

        logger.info("PowerBI Integration Tool initialized")
        logger.info(f"Host: {self._host}")
        logger.info(f"Power BI Job ID: {self._job_id}")
        if self._token:
            masked_token = f"{self._token[:4]}...{self._token[-4:]}" if len(self._token) > 8 else "***"
            logger.info(f"Token (masked): {masked_token}")

    async def _make_api_call(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make API call to Databricks REST API."""
        url = f"https://{self._host}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=method,
                url=url,
                json=data if data else None,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"API call failed: {response.status} - {await response.text()}")

    def _run(
        self,
        question: str,
        semantic_model_id: str,
        workspace_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        auth_method: str = "service_principal",
        databricks_host: Optional[str] = None,
        databricks_token: Optional[str] = None,
        timeout_seconds: int = 600
    ) -> str:
        """
        Execute Power BI query end-to-end.

        Args:
            question: Natural language question
            semantic_model_id: Power BI semantic model ID
            workspace_id: Power BI workspace ID
            tenant_id: Azure AD tenant ID
            client_id: Azure AD client ID
            client_secret: Azure AD client secret
            auth_method: Authentication method ('service_principal' or 'device_code')
            databricks_host: Optional custom host for notebook (overrides tool config)
            databricks_token: Optional custom token for notebook (overrides tool config)
            timeout_seconds: Maximum time to wait for completion

        Returns:
            JSON string containing DAX execution results
        """
        try:
            logger.info(f"PowerBI Tool - Executing query: {question[:100]}")
            logger.info(f"  Semantic Model: {semantic_model_id}")
            logger.info(f"  Workspace: {workspace_id}")

            # Deduplication: Generate request hash
            request_signature = f"{question}|{semantic_model_id}|{workspace_id}|{tenant_id}"
            request_hash = hashlib.md5(request_signature.encode()).hexdigest()

            # Check cache for recent identical requests (within last 30 minutes)
            cache_ttl = timedelta(minutes=30)
            current_time = datetime.now()

            # Clean expired cache entries
            expired_keys = [
                key for key, (_, timestamp, _) in _JOB_SUBMISSION_CACHE.items()
                if current_time - timestamp > cache_ttl
            ]
            for key in expired_keys:
                del _JOB_SUBMISSION_CACHE[key]

            # Check if this exact request was made recently
            if request_hash in _JOB_SUBMISSION_CACHE:
                run_id, cached_time, cached_result = _JOB_SUBMISSION_CACHE[request_hash]
                age_seconds = (current_time - cached_time).total_seconds()
                logger.warning(
                    f"⚠️ DUPLICATE REQUEST DETECTED - Returning cached result from {age_seconds:.1f}s ago"
                )
                logger.warning(f"   Original run_id: {run_id}")
                logger.warning(f"   Request hash: {request_hash[:8]}...")
                return cached_result

            # Check authentication
            if not self._token:
                return json.dumps({
                    "error": "No authentication available",
                    "details": "Please configure DATABRICKS_API_KEY or token"
                })

            # Check job ID
            if not self._job_id:
                return json.dumps({
                    "error": "Power BI job ID not configured",
                    "details": "Please provide powerbi_job_id in tool_config"
                })

            # Create event loop for async operations
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Execute the full pipeline
                result = loop.run_until_complete(
                    self._execute_powerbi_pipeline(
                        question=question,
                        semantic_model_id=semantic_model_id,
                        workspace_id=workspace_id,
                        tenant_id=tenant_id,
                        client_id=client_id,
                        client_secret=client_secret,
                        auth_method=auth_method,
                        databricks_host=databricks_host,
                        databricks_token=databricks_token,
                        timeout_seconds=timeout_seconds
                    )
                )

                # Cache the successful result to prevent duplicate submissions
                result_data = json.loads(result)
                if result_data.get("success"):
                    run_id = result_data.get("run_id")
                    _JOB_SUBMISSION_CACHE[request_hash] = (run_id, current_time, result)
                    logger.info(f"✅ Cached result for request hash: {request_hash[:8]}...")

                return result

            finally:
                loop.close()

        except Exception as e:
            logger.error(f"Error in Power BI tool: {str(e)}", exc_info=True)
            return json.dumps({
                "error": f"Failed to execute Power BI query: {str(e)}"
            })

    async def _execute_powerbi_pipeline(
        self,
        question: str,
        semantic_model_id: str,
        workspace_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        auth_method: str,
        databricks_host: Optional[str],
        databricks_token: Optional[str],
        timeout_seconds: int
    ) -> str:
        """
        Execute the full Power BI pipeline: submit job, wait, extract results.

        Returns:
            JSON string with execution results
        """
        start_time = time.time()

        # Use provided host/token or fall back to tool config
        # If databricks_host is provided, use it as-is (user should include https://)
        # If not provided, use self._host with https:// prefix
        notebook_host = databricks_host if databricks_host else f"https://{self._host}"
        notebook_token = databricks_token if databricks_token else self._token

        # Step 1: Submit the job
        logger.info("Step 1: Submitting Power BI pipeline job...")
        logger.info(f"  Auth Method: {auth_method}")
        logger.info(f"  Databricks Host for notebook: {notebook_host}")
        logger.info(f"  Using custom token: {databricks_token is not None}")

        job_params = {
            "question": question,
            "semantic_model_id": semantic_model_id,
            "workspace_id": workspace_id,
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_method": auth_method,
            "databricks_host": notebook_host,
            "databricks_token": notebook_token
        }

        # Submit job via Databricks Jobs API
        payload = {
            "job_id": self._job_id,
            "job_parameters": {
                "job_params": json.dumps(job_params)
            }
        }

        submit_response = await self._make_api_call("POST", "/api/2.1/jobs/run-now", payload)
        run_id = submit_response.get("run_id")

        if not run_id:
            return json.dumps({
                "error": "No run_id returned from job submission",
                "details": submit_response
            })

        logger.info(f"✅ Job submitted successfully: run_id={run_id}")

        # Step 2: Wait for completion
        logger.info(f"Step 2: Waiting for job completion (timeout: {timeout_seconds}s)...")
        completed = await self._wait_for_completion_async(int(run_id), timeout_seconds)

        if not completed:
            elapsed = time.time() - start_time
            return json.dumps({
                "error": "Job did not complete within timeout",
                "run_id": run_id,
                "timeout_seconds": timeout_seconds,
                "elapsed_seconds": round(elapsed, 1),
                "details": "The job is still running or failed. Check Databricks UI for details."
            })

        # Step 3: Extract results
        logger.info("Step 3: Extracting results from job output...")

        # Get run info to handle multi-task jobs
        run_info = await self._make_api_call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")

        # Handle multi-task job structure
        tasks = run_info.get("tasks", [])
        actual_run_id = run_id

        # Hardcoded task_key for Power BI pipeline
        POWERBI_TASK_KEY = "pbi_e2e_pipeline"

        if tasks and len(tasks) > 0:
            # Find the pbi_e2e_pipeline task
            task_run = None
            for task in tasks:
                if task.get("task_key") == POWERBI_TASK_KEY:
                    task_run = task
                    break

            if task_run:
                actual_run_id = str(task_run.get("run_id"))
                logger.info(f"Using task run_id: {actual_run_id} for task '{POWERBI_TASK_KEY}'")
            else:
                # Fallback to first task
                actual_run_id = str(tasks[0].get("run_id"))
                logger.warning(f"Task '{POWERBI_TASK_KEY}' not found, using first task")

        # Fetch output
        output_response = await self._make_api_call(
            "GET",
            f"/api/2.1/jobs/runs/get-output?run_id={actual_run_id}"
        )

        notebook_output = output_response.get("notebook_output")
        if not notebook_output or not notebook_output.get("result"):
            return json.dumps({
                "error": "No output found from job run",
                "run_id": run_id,
                "details": "The notebook may not have called dbutils.notebook.exit() or the job failed."
            })

        # Parse result
        result_data = json.loads(notebook_output.get("result"))

        # Extract DAX execution results (hardcoded extract_key)
        EXTRACT_KEY = "pipeline_steps.step_3_execution.result_data"
        extracted_data = self._extract_nested_key(result_data, EXTRACT_KEY)

        elapsed = time.time() - start_time

        if extracted_data is None:
            available_keys = self._get_available_keys(result_data)
            return json.dumps({
                "error": f"Key '{EXTRACT_KEY}' not found in job output",
                "run_id": run_id,
                "elapsed_seconds": round(elapsed, 1),
                "available_keys": available_keys
            })

        logger.info(f"✅ Power BI pipeline completed successfully in {elapsed:.1f}s")

        # Return comprehensive result
        return json.dumps({
            "success": True,
            "run_id": run_id,
            "question": question,
            "elapsed_seconds": round(elapsed, 1),
            "dax_query": result_data.get("pipeline_steps", {}).get("step_2_dax_generation", {}).get("dax_query"),
            "result_data": extracted_data,
            "message": f"Successfully executed Power BI query in {elapsed:.1f}s"
        }, indent=2)

    async def _wait_for_completion_async(self, run_id: int, timeout_seconds: int) -> bool:
        """Wait for job run to complete using async API calls."""
        start_time = time.time()
        check_interval = 5

        while True:
            elapsed = time.time() - start_time

            if elapsed > timeout_seconds:
                logger.warning(f"Timeout waiting for run_id {run_id}")
                return False

            try:
                run_info = await self._make_api_call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
                state = run_info.get("state", {})
                life_cycle_state = state.get("life_cycle_state")
                result_state = state.get("result_state")

                terminal_states = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]

                if life_cycle_state in terminal_states:
                    if result_state == "SUCCESS":
                        logger.info(f"✅ Job run {run_id} completed successfully after {elapsed:.1f}s")
                        return True
                    else:
                        logger.error(f"❌ Job run {run_id} failed with state: {result_state}")
                        return False

                logger.info(f"⏳ Job run {run_id} is {life_cycle_state} (waited {elapsed:.1f}s)")
                await asyncio.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error checking run status: {str(e)}")
                return False

    def _extract_nested_key(self, data: Dict[str, Any], key_path: str) -> Any:
        """Extract nested key using dot notation."""
        keys = key_path.split('.')
        current = data

        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None

        return current

    def _get_available_keys(self, data: Any, prefix: str = "", max_depth: int = 3, current_depth: int = 0) -> list:
        """Get available keys in nested structure."""
        if current_depth >= max_depth:
            return []

        keys = []

        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                keys.append(full_key)

                if isinstance(value, dict):
                    nested_keys = self._get_available_keys(value, full_key, max_depth, current_depth + 1)
                    keys.extend(nested_keys)

        return keys
