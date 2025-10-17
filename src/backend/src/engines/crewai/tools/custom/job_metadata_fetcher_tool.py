"""
Job Metadata Fetcher Tool

CrewAI tool that fetches output from a completed Databricks job run.
This tool extracts the notebook exit output from any job that uses dbutils.notebook.exit().

Features:
- Waits for job completion before fetching output
- Supports multi-task jobs
- Supports nested key extraction (e.g., 'pipeline_steps.step_3_execution.result_data')
- Uses same authentication system as DatabricksJobsTool (PAT / API Keys Service)

Generic tool that can be used for any job type, not just Power BI metadata extraction.
"""

import json
import logging
import time
import os
import asyncio
from typing import Any, Dict, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import aiohttp

logger = logging.getLogger(__name__)


class JobMetadataFetcherSchema(BaseModel):
    """Input schema for Job Metadata Fetcher Tool."""

    run_id: str = Field(
        ...,
        description="Databricks job run ID from the completed job"
    )
    task_key: Optional[str] = Field(
        None,
        description="Optional task key for multi-task jobs. If the job has multiple tasks, specify which task's output to retrieve. "
                    "If not provided and job has only one task, will auto-detect."
    )
    extract_key: Optional[str] = Field(
        None,
        description="Optional key to extract from the result. Supports nested keys using dot notation "
                    "(e.g., 'metadata', 'compact_metadata', 'pipeline_steps.step_3_execution.result_data'). "
                    "If not provided, returns the entire result."
    )
    wait_for_completion: Optional[bool] = Field(
        True,
        description="Whether to wait for job completion before fetching output. Default is True. "
                    "If False and job is not complete, will return an error."
    )
    timeout_seconds: Optional[int] = Field(
        600,
        description="Maximum time in seconds to wait for job completion. Default is 600 (10 minutes)."
    )


class JobMetadataFetcherTool(BaseTool):
    """
    Job Metadata Fetcher Tool for CrewAI agents.

    This tool fetches the notebook exit output from a completed Databricks job.
    Use this after running any notebook job via DatabricksJobsTool that exits with data.

    The tool expects jobs to use: dbutils.notebook.exit(json.dumps(result_data))

    Example workflow:
        1. Agent 1 uses DatabricksJobsTool to run a notebook job
        2. Agent 1 uses JobMetadataFetcherTool to get the output from run_id
        3. Agent 2 receives the output and processes it further

    Example notebook exit:
        result = {"status": "success", "metadata": {...}, "tables_count": 5}
        dbutils.notebook.exit(json.dumps(result))
    """

    name: str = "Job Metadata Fetcher"
    description: str = (
        "Fetch output from a Databricks notebook job. "
        "Provide the run_id from the job, and this tool will wait for completion "
        "and return the data that was passed to dbutils.notebook.exit(). "
        "For multi-task jobs, optionally specify task_key to retrieve output from a specific task. "
        "Supports nested key extraction using dot notation (e.g., 'pipeline_steps.step_3_execution.result_data')."
    )
    args_schema: Type[BaseModel] = JobMetadataFetcherSchema

    # Private attributes for authentication (similar to DatabricksJobsTool)
    _host: str = PrivateAttr(default=None)
    _token: str = PrivateAttr(default=None)

    def __init__(
        self,
        databricks_host: Optional[str] = None,
        tool_config: Optional[dict] = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize the Job Metadata Fetcher Tool.

        Args:
            databricks_host: Databricks workspace host URL
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

        logger.info("JobMetadataFetcherTool initialized")
        logger.info(f"Host: {self._host}")
        if self._token:
            masked_token = f"{self._token[:4]}...{self._token[-4:]}" if len(self._token) > 8 else "***"
            logger.info(f"Token (masked): {masked_token}")

    async def _make_api_call(self, endpoint: str) -> Dict[str, Any]:
        """Make API call to Databricks REST API."""
        url = f"https://{self._host}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"API call failed: {response.status} - {await response.text()}")

    def _run(
        self,
        run_id: str,
        task_key: Optional[str] = None,
        extract_key: Optional[str] = None,
        wait_for_completion: bool = True,
        timeout_seconds: int = 600,
        **kwargs: Any
    ) -> str:
        """
        Fetch output from job run.

        Args:
            run_id: Databricks job run ID
            task_key: Optional task key for multi-task jobs
            extract_key: Optional key to extract from result (supports dot notation for nested keys)
            wait_for_completion: Whether to wait for job completion (default: True)
            timeout_seconds: Maximum time to wait for completion in seconds (default: 600)

        Returns:
            JSON string containing job output or extracted field
        """
        try:
            logger.info(f"Fetching output from run_id: {run_id}")

            # Check authentication
            if not self._token:
                return json.dumps({
                    "error": "No authentication available",
                    "details": "Please configure DATABRICKS_API_KEY or token"
                })

            # Create event loop for async operations
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Step 1: Wait for job completion if requested
                if wait_for_completion:
                    logger.info("Waiting for job completion...")
                    completed = loop.run_until_complete(self._wait_for_completion_async(int(run_id), timeout_seconds))
                    if not completed:
                        return json.dumps({
                            "error": "Job did not complete within timeout",
                            "run_id": run_id,
                            "timeout_seconds": timeout_seconds,
                            "details": "The job is still running or pending. Increase timeout_seconds or check job status."
                        })

                # Get run information
                run_info = loop.run_until_complete(self._make_api_call(f"/api/2.1/jobs/runs/get?run_id={run_id}"))

                # Determine which run_id to use for fetching output
                tasks = run_info.get("tasks", [])
                actual_run_id = run_id

                if tasks and len(tasks) > 1:
                    # Multi-task job
                    logger.info(f"Multi-task job detected with {len(tasks)} tasks")

                    if not task_key:
                        task_key = tasks[0].get("task_key")
                        logger.info(f"Auto-detected task_key: {task_key}")

                    # Find the task
                    task_run = None
                    for task in tasks:
                        if task.get("task_key") == task_key:
                            task_run = task
                            break

                    if not task_run:
                        available_tasks = [t.get("task_key") for t in tasks]
                        return json.dumps({
                            "error": f"Task '{task_key}' not found in job run",
                            "run_id": run_id,
                            "available_tasks": available_tasks
                        })

                    actual_run_id = str(task_run.get("run_id"))
                    logger.info(f"Using task run_id: {actual_run_id}")

                elif tasks and len(tasks) == 1:
                    # Single task job
                    task_run = tasks[0]
                    task_key = task_run.get("task_key")
                    actual_run_id = str(task_run.get("run_id"))
                    logger.info(f"Single task job: {task_key}, using run_id: {actual_run_id}")

                # Fetch output
                output_response = loop.run_until_complete(
                    self._make_api_call(f"/api/2.1/jobs/runs/get-output?run_id={actual_run_id}")
                )

                notebook_output = output_response.get("notebook_output")
                if not notebook_output or not notebook_output.get("result"):
                    return json.dumps({
                        "error": "No output found from job run",
                        "run_id": run_id,
                        "task_key": task_key,
                        "details": "The notebook may not have called dbutils.notebook.exit() or the job failed."
                    })

                # Parse result
                result_data = json.loads(notebook_output.get("result"))

                # Extract specific key if requested
                if extract_key:
                    extracted_data = self._extract_nested_key(result_data, extract_key)

                    if extracted_data is None:
                        available_keys = self._get_available_keys(result_data)
                        return json.dumps({
                            "error": f"Key '{extract_key}' not found in job output",
                            "run_id": run_id,
                            "available_keys": available_keys
                        })

                    logger.info(f"✅ Extracted '{extract_key}' from job output")

                    return json.dumps({
                        "success": True,
                        "run_id": run_id,
                        "task_key": task_key,
                        "extract_key": extract_key,
                        "data": extracted_data,
                        "message": f"Successfully extracted '{extract_key}' from job output"
                    }, indent=2)

                # Return full result
                logger.info(f"✅ Fetched complete job output with keys: {list(result_data.keys())}")

                return json.dumps({
                    "success": True,
                    "run_id": run_id,
                    "task_key": task_key,
                    "data": result_data,
                    "available_keys": list(result_data.keys()),
                    "message": "Successfully fetched complete job output"
                }, indent=2)

            finally:
                loop.close()

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse job output as JSON: {str(e)}")
            return json.dumps({
                "error": "Job output is not valid JSON",
                "run_id": run_id,
                "details": str(e)
            })
        except Exception as e:
            logger.error(f"Error fetching output from run_id {run_id}: {str(e)}", exc_info=True)
            return json.dumps({
                "error": f"Failed to fetch job output: {str(e)}",
                "run_id": run_id
            })

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
                run_info = await self._make_api_call(f"/api/2.1/jobs/runs/get?run_id={run_id}")
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
