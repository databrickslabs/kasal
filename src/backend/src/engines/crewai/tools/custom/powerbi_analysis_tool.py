import logging
import asyncio
import json
import time
import re
import os
from datetime import datetime
from typing import Any, Optional, Type
from concurrent.futures import ThreadPoolExecutor

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr, model_validator

logger = logging.getLogger(__name__)

# Emergency debug logging to file (bypasses all logging config)
def _debug_log(msg: str):
    """Write debug message directly to file, bypassing logger."""
    try:
        debug_file = '/tmp/powerbi_tool_debug.log'
        with open(debug_file, 'a') as f:
            timestamp = datetime.now().isoformat()
            f.write(f"[{timestamp}] {msg}\n")
            f.flush()
    except Exception:
        pass  # Silently fail if we can't write debug log

# Thread pool executor for running async operations from sync context
_EXECUTOR = ThreadPoolExecutor(max_workers=5)


def _run_async_in_sync_context(coro):
    """
    Safely run an async coroutine from a synchronous context.

    This handles the case where we're already in an event loop (e.g., FastAPI)
    and need to execute async code from a sync function (e.g., CrewAI tool's _run method).

    Args:
        coro: The coroutine to execute

    Returns:
        The result of the coroutine execution
    """
    try:
        # Try to get the current running loop
        loop = asyncio.get_running_loop()
        # We're already in an async context, run in executor to avoid nested loop issues
        logger.debug("Detected running event loop, using ThreadPoolExecutor")
        future = _EXECUTOR.submit(asyncio.run, coro)
        return future.result()
    except RuntimeError:
        # No event loop running, we can safely create one
        logger.debug("No running event loop detected, creating new loop")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class PowerBIAnalysisToolSchema(BaseModel):
    """Input schema for PowerBIAnalysisTool."""

    dashboard_id: str = Field(
        ..., description="Power BI dashboard/semantic model ID to analyze"
    )
    questions: list = Field(
        ..., description="Business questions to analyze using DAX"
    )
    workspace_id: Optional[str] = Field(
        None, description="Power BI workspace ID (uses default if not provided)"
    )
    dax_statement: Optional[str] = Field(
        None, description="Pre-generated DAX statement (optional, will be generated if not provided)"
    )
    job_id: Optional[int] = Field(
        None, description="Databricks job ID to execute (overrides configured job_id if provided)"
    )
    additional_params: Optional[dict] = Field(
        None, description=(
            "Additional parameters for Power BI authentication and Databricks job execution. "
            "Required fields: 'tenant_id' (Azure AD Tenant ID), 'client_id' (Azure AD Application ID). "
            "Optional fields: 'auth_method' (default: 'service_principal'), 'sample_size', 'metadata', 'task_key'. "
            "NOTE: Credentials auto-fetched from API Keys: POWERBI_CLIENT_SECRET, POWERBI_USERNAME, POWERBI_PASSWORD, DATABRICKS_TOKEN. "
            "Databricks host auto-detected from environment. "
            "Example: {'tenant_id': 'xxx', 'client_id': 'yyy', 'auth_method': 'service_principal'}"
        )
    )

    @model_validator(mode='after')
    def validate_input(self) -> 'PowerBIAnalysisToolSchema':
        """Validate the input parameters."""
        if not self.questions and not self.dax_statement:
            raise ValueError("Either 'questions' or 'dax_statement' must be provided")
        return self


class PowerBIAnalysisTool(BaseTool):
    """
    A tool for complex Power BI analysis using Databricks job execution.

    This tool is designed for:
    - Heavy computational analysis
    - Long-running DAX queries
    - Complex data transformations
    - Result persistence to Databricks volumes
    - Integration with other data sources


    Architecture:
    1. Accepts business questions or DAX statements
    2. Triggers Databricks job with parameters
    3. Databricks notebook executes DAX against Power BI
    4. Results are processed and optionally stored
    5. Returns analysis results to agent
    """

    name: str = "Power BI Analysis (Databricks)"
    description: str = (
        "Execute complex Power BI analysis using Databricks jobs. "
        "Suitable for heavy computations, long-running queries, and advanced analytics. "
        "\n\nREQUIRED PARAMETERS:\n"
        "- 'job_id': Databricks job ID to execute (can be set in tool config as default)\n"
        "- 'dashboard_id': Power BI semantic model ID to query\n"
        "- 'questions': List of business questions to analyze\n"
        "- 'additional_params': Dict with Power BI authentication:\n"
        "  - 'tenant_id': Azure AD Tenant ID (required)\n"
        "  - 'client_id': Azure AD Application ID (required)\n"
        "  - 'auth_method': Authentication method (default: 'service_principal')\n"
        "  - NOTE: Credentials auto-fetched from API Keys:\n"
        "    - POWERBI_CLIENT_SECRET (for service_principal auth)\n"
        "    - POWERBI_USERNAME (optional, for device_code auth)\n"
        "    - POWERBI_PASSWORD (optional, for device_code auth)\n"
        "    - DATABRICKS_TOKEN (for Databricks API access)\n"
        "\n\nOPTIONAL PARAMETERS:\n"
        "- 'workspace_id': Power BI workspace ID\n"
        "- 'dax_statement': Pre-generated DAX query\n"
        "- Additional params: 'databricks_host', 'databricks_token', 'sample_size', 'metadata', "
        "'task_key' (default: 'pbi_e2e_pipeline' for multi-task jobs)\n"
        "\n\nEXAMPLE:\n"
        "job_id=365257288725339, dashboard_id='a17de62e-...', questions=['What is total NSR?'], "
        "additional_params={'tenant_id': 'xxx-xxx', 'client_id': 'yyy-yyy', 'auth_method': 'service_principal'}"
    )
    args_schema: Type[BaseModel] = PowerBIAnalysisToolSchema

    _group_id: Optional[str] = PrivateAttr(default=None)
    _databricks_job_id: Optional[int] = PrivateAttr(default=None)
    _tenant_id: Optional[str] = PrivateAttr(default=None)
    _client_id: Optional[str] = PrivateAttr(default=None)
    _workspace_id: Optional[str] = PrivateAttr(default=None)
    _semantic_model_id: Optional[str] = PrivateAttr(default=None)
    _auth_method: Optional[str] = PrivateAttr(default="service_principal")

    def __init__(
        self,
        group_id: Optional[str] = None,
        databricks_job_id: Optional[int] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        auth_method: Optional[str] = "service_principal",
        **kwargs
    ):
        """
        Initialize PowerBIAnalysisTool.

        Args:
            group_id: Group ID for multi-tenant support
            databricks_job_id: Databricks job ID for Power BI analysis (if pre-configured)
            tenant_id: Azure AD Tenant ID for Power BI authentication
            client_id: Azure AD Application/Client ID for Power BI authentication
            workspace_id: Default Power BI Workspace ID (optional, can be overridden per task)
            semantic_model_id: Default Power BI Semantic Model ID (optional, can be overridden per task)
            auth_method: Authentication method ("service_principal" or "device_code")
            **kwargs: Additional keyword arguments for BaseTool
        """
        super().__init__(**kwargs)
        self._group_id = group_id
        self._databricks_job_id = databricks_job_id
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._workspace_id = workspace_id
        self._semantic_model_id = semantic_model_id
        self._auth_method = auth_method

        # Clear debug log file at initialization
        try:
            debug_file = '/tmp/powerbi_tool_debug.log'
            if os.path.exists(debug_file):
                os.remove(debug_file)
        except Exception:
            pass

        _debug_log(f"PowerBIAnalysisTool initialized for group: {group_id or 'default'}, job_id: {databricks_job_id}")
        logger.info(f"PowerBIAnalysisTool initialized for group: {group_id or 'default'}")

    def _run(self, **kwargs: Any) -> str:
        """
        Execute a Power BI analysis action via Databricks job.

        Args:
            job_id (Optional[int]): Databricks job ID to execute (overrides configured job_id)
            dashboard_id (str): Power BI dashboard/semantic model ID
            questions (list): Business questions to analyze
            workspace_id (Optional[str]): Workspace ID
            dax_statement (Optional[str]): Pre-generated DAX statement
            additional_params (Optional[dict]): Additional parameters for the Databricks job
                (e.g., auth_method, tenant_id, client_id, client_secret, sample_size, etc.)

        Returns:
            str: Formatted analysis results
        """
        _debug_log(f"PowerBI tool _run called with job_id={kwargs.get('job_id')}, dashboard_id={kwargs.get('dashboard_id')}")
        logger.info(f"[POWERBI-TOOL] _run called with job_id={kwargs.get('job_id')}")
        # Use helper function to safely run async code from sync context
        result = _run_async_in_sync_context(self._execute_analysis(**kwargs))
        _debug_log(f"PowerBI tool _run returning result of type {type(result)}, length={len(str(result))}")
        logger.info(f"[POWERBI-TOOL] _run returning result length={len(str(result))}")
        return result

    async def _execute_analysis(self, **kwargs) -> str:
        """
        Async implementation of analysis execution via Databricks.

        Args:
            **kwargs: Analysis parameters including job_id, dashboard_id, questions, etc.

        Returns:
            str: Formatted analysis results
        """
        dashboard_id = kwargs.get('dashboard_id')
        questions = kwargs.get('questions', [])
        workspace_id = kwargs.get('workspace_id')
        job_id = kwargs.get('job_id')  # Get job_id from parameters
        additional_params = kwargs.get('additional_params')  # Get additional parameters

        _debug_log(f"_execute_analysis started: job_id={job_id}, dashboard_id={dashboard_id}, questions={questions}")
        logger.info(f"[POWERBI-TOOL] _execute_analysis started")

        try:
            # Import here to avoid circular dependency
            from .databricks_jobs_tool import DatabricksJobsTool

            # Auto-detect Databricks configuration
            databricks_host = None
            databricks_token = None
            tool_config = {}

            # 1. Try to get databricks_host from unified auth (auto-detect from environment)
            try:
                from src.utils.databricks_auth import get_auth_context
                auth_context = await get_auth_context()
                if auth_context and auth_context.workspace_url:
                    databricks_host = auth_context.workspace_url
                    logger.info(f"Auto-detected databricks_host from environment: {databricks_host}")
            except Exception as e:
                logger.debug(f"Could not auto-detect databricks_host from auth context: {e}")

            # 2. Override with additional_params if explicitly provided
            if additional_params and 'databricks_host' in additional_params:
                databricks_host = additional_params['databricks_host']
                logger.info(f"Using databricks_host from additional_params: {databricks_host}")

            # 3. Try to get databricks_token from API Keys first (secure)
            try:
                from src.services.api_keys_service import ApiKeysService
                from src.db.session import async_session_factory
                from src.utils.encryption_utils import EncryptionUtils

                async with async_session_factory() as session:
                    api_keys_service = ApiKeysService(session, group_id=self._group_id)
                    # Try both DATABRICKS_TOKEN and DATABRICKS_API_KEY
                    token_obj = await api_keys_service.find_by_name("DATABRICKS_TOKEN")
                    if not token_obj:
                        token_obj = await api_keys_service.find_by_name("DATABRICKS_API_KEY")

                    if token_obj and token_obj.encrypted_value:
                        databricks_token = EncryptionUtils.decrypt_value(token_obj.encrypted_value)
                        logger.info("Retrieved databricks_token from API Keys")
            except Exception as e:
                logger.debug(f"Could not retrieve databricks_token from API Keys: {e}")

            # 4. Fall back to additional_params if not found in API Keys
            if not databricks_token and additional_params and 'databricks_token' in additional_params:
                databricks_token = additional_params['databricks_token']
                logger.info("Using databricks_token from additional_params")

            # 5. Build tool_config for DatabricksJobsTool
            if databricks_token:
                tool_config['DATABRICKS_API_KEY'] = databricks_token

            if databricks_host:
                tool_config['DATABRICKS_HOST'] = databricks_host

            # Create DatabricksJobsTool instance with proper configuration
            databricks_tool = DatabricksJobsTool(
                databricks_host=databricks_host,
                tool_config=tool_config,
                token_required=False  # We handle auth through tool_config
            )

            # Prepare job parameters with correct field names for Databricks job
            # The job expects:
            # - "question" (singular string, not "questions" array)
            # - "semantic_model_id" (not "dashboard_id")
            # - No "dax_statement" field

            # Convert questions array to single question string (take first question)
            question_str = questions[0] if questions and len(questions) > 0 else ""

            job_params = {
                "question": question_str,                    # Singular, not plural
                "semantic_model_id": dashboard_id,           # Renamed from dashboard_id
                "workspace_id": workspace_id,
                # dax_statement is NOT sent to the job (internal to PowerBI tool)
            }

            logger.info(f"Prepared job parameters with question: '{question_str[:50]}...' and semantic_model_id: {dashboard_id}")

            # Build PowerBI configuration with precedence: tool config (from task) > additional_params (from LLM)
            powerbi_config = {}

            # PRIORITY 1: Use tool initialization values (from task config) - HIGHEST PRIORITY
            if self._tenant_id:
                powerbi_config['tenant_id'] = self._tenant_id
            if self._client_id:
                powerbi_config['client_id'] = self._client_id
            if self._auth_method:
                powerbi_config['auth_method'] = self._auth_method
            if self._workspace_id:
                powerbi_config['workspace_id'] = self._workspace_id

            # PRIORITY 2: Fall back to additional_params only if not already set (from LLM - lower priority)
            if additional_params:
                if 'tenant_id' not in powerbi_config and 'tenant_id' in additional_params:
                    powerbi_config['tenant_id'] = additional_params['tenant_id']
                if 'client_id' not in powerbi_config and 'client_id' in additional_params:
                    powerbi_config['client_id'] = additional_params['client_id']
                if 'auth_method' not in powerbi_config and 'auth_method' in additional_params:
                    powerbi_config['auth_method'] = additional_params['auth_method']
                if 'workspace_id' not in powerbi_config and 'workspace_id' in additional_params:
                    powerbi_config['workspace_id'] = additional_params['workspace_id']

            # Use workspace_id from kwargs if provided (override everything)
            if workspace_id:
                powerbi_config['workspace_id'] = workspace_id

            # Use semantic_model_id from kwargs (dashboard_id) if provided, otherwise use default from config
            # Note: dashboard_id in kwargs takes precedence over semantic_model_id from config
            if not dashboard_id and self._semantic_model_id:
                job_params['semantic_model_id'] = self._semantic_model_id

            logger.info(f"PowerBI config (task-level overrides applied): {list(powerbi_config.keys())}")

            # Fetch PowerBI credentials from API Keys service (encrypted storage)
            # These are sensitive and should never be stored in task config or passed in plain text
            try:
                from src.services.api_keys_service import ApiKeysService
                from src.db.session import async_session_factory
                from src.utils.encryption_utils import EncryptionUtils

                async with async_session_factory() as session:
                    # Use group_id for multi-tenant isolation
                    api_keys_service = ApiKeysService(session, group_id=self._group_id)

                    # Fetch client_secret (required for service_principal auth)
                    client_secret_obj = await api_keys_service.find_by_name("POWERBI_CLIENT_SECRET")
                    if client_secret_obj and client_secret_obj.encrypted_value:
                        client_secret = EncryptionUtils.decrypt_value(client_secret_obj.encrypted_value)
                        powerbi_config['client_secret'] = client_secret
                        logger.info("Successfully retrieved POWERBI_CLIENT_SECRET from API Keys")
                    else:
                        logger.warning("POWERBI_CLIENT_SECRET not found in API Keys")

                    # Fetch username (optional, for device_code or interactive auth)
                    username_obj = await api_keys_service.find_by_name("POWERBI_USERNAME")
                    if username_obj and username_obj.encrypted_value:
                        username = EncryptionUtils.decrypt_value(username_obj.encrypted_value)
                        powerbi_config['username'] = username
                        logger.info("Successfully retrieved POWERBI_USERNAME from API Keys")

                    # Fetch password (optional, for device_code or interactive auth)
                    password_obj = await api_keys_service.find_by_name("POWERBI_PASSWORD")
                    if password_obj and password_obj.encrypted_value:
                        password = EncryptionUtils.decrypt_value(password_obj.encrypted_value)
                        powerbi_config['password'] = password
                        logger.info("Successfully retrieved POWERBI_PASSWORD from API Keys")

            except Exception as e:
                logger.error(f"Error retrieving PowerBI credentials from API Keys: {e}")
                # Continue without credentials - they might be provided via additional_params

            # Merge additional parameters (these will be passed to the Databricks notebook/job)
            if additional_params:
                # Create a copy to avoid modifying the original
                job_additional_params = additional_params.copy()

                # Remove auth params that we already extracted (they're in powerbi_config or tool_config)
                # This avoids duplication in job_params
                for key in ['tenant_id', 'client_id', 'auth_method', 'client_secret', 'username', 'password', 'databricks_host', 'databricks_token']:
                    job_additional_params.pop(key, None)

                job_params.update(job_additional_params)
                logger.info(f"Added {len(job_additional_params)} additional parameters to job_params")

            # Merge PowerBI config into job_params
            job_params.update(powerbi_config)

            # Determine which job_id to use: parameter takes precedence over configured value
            effective_job_id = job_id if job_id is not None else self._databricks_job_id

            # If job_id is available (from parameter or configuration), run it; otherwise, return instructions
            if effective_job_id:
                logger.info(f"Running Databricks job {effective_job_id} for Power BI analysis")
                logger.info(f"Job parameters: {list(job_params.keys())}")
                logger.info(f"Databricks host configured: {databricks_host}")
                logger.info(f"Databricks token configured: {'Yes' if databricks_token else 'No'}")

                # Trigger job run
                logger.info(f"🚀 Triggering Databricks job {effective_job_id} with {len(job_params)} parameters")
                run_result = databricks_tool._run(
                    action="run",
                    job_id=effective_job_id,
                    job_params=job_params
                )

                logger.info(f"📝 Job trigger result: {run_result[:200]}...")

                # Parse run_id from result
                # Expected format: "✅ Job run started successfully\nRun ID: 12345\n..."
                run_id = self._extract_run_id(run_result)
                logger.info(f"📍 Extracted run_id: {run_id}")

                if run_id:
                    # Get the task_key from additional_params if provided, otherwise use default
                    task_key = additional_params.get('task_key', 'pbi_e2e_pipeline') if additional_params else 'pbi_e2e_pipeline'
                    _debug_log(f"Monitoring Databricks job run {run_id}, task: {task_key}")
                    logger.info(f"[POWERBI-TOOL] Monitoring Databricks job run {run_id}, task: {task_key}")

                    # Poll for completion
                    max_wait = 300  # 5 minutes
                    poll_interval = 5  # 5 seconds
                    elapsed = 0

                    while elapsed < max_wait:
                        # Check task status directly (for multi-task jobs)
                        task_status = await self._check_task_status(databricks_tool, run_id, task_key)

                        logger.warning(f"⏱️ Task '{task_key}' status: {task_status} (elapsed: {elapsed}s/{max_wait}s)")

                        if task_status == "SUCCESS":
                            # Task completed successfully - extract the notebook output
                            _debug_log(f"Task '{task_key}' SUCCESS - extracting output from run_id={run_id}")
                            logger.info(f"[POWERBI-TOOL] 🎯 Task '{task_key}' completed successfully (run_id: {run_id}), extracting notebook output...")

                            try:
                                # Get the notebook output by calling the Databricks API directly
                                _debug_log(f"Calling _get_notebook_output with run_id={run_id}, task_key={task_key}")
                                logger.info(f"[POWERBI-TOOL] Calling _get_notebook_output with run_id={run_id}, task_key={task_key}")
                                result_data = await self._get_notebook_output(databricks_tool, run_id, task_key)
                                _debug_log(f"_get_notebook_output returned: {type(result_data)} - has_data={bool(result_data)}")
                                logger.info(f"[POWERBI-TOOL] _get_notebook_output returned: {type(result_data)} - {bool(result_data)}")

                                if result_data:
                                    rows_count = result_data.get('rows_returned', 0)
                                    _debug_log(f"Successfully extracted {rows_count} rows from task output")
                                    logger.info(f"[POWERBI-TOOL] ✅ Successfully extracted {rows_count} rows from task output")
                                    formatted_result = self._format_analysis_result(
                                        dashboard_id,
                                        question_str,
                                        result_data
                                    )
                                    _debug_log(f"Formatted result length: {len(formatted_result)} chars")
                                    logger.info(f"[POWERBI-TOOL] Formatted result length: {len(formatted_result)} chars")
                                    logger.info(f"[POWERBI-TOOL] Result preview: {formatted_result[:500]}")
                                    _debug_log(f"Returning formatted result: {formatted_result[:200]}...")
                                    return formatted_result
                                else:
                                    # Fallback to basic success message if we can't extract data
                                    # Return detailed debug info to help diagnose the issue
                                    logger.error(f"[POWERBI-TOOL] ❌ result_data is None/empty")
                                    logger.error(f"[POWERBI-TOOL] Failed to extract result data from task '{task_key}' (run_id: {run_id})")

                                    # Include debug information in the response
                                    debug_info = f"✅ Task '{task_key}' completed successfully but could not extract detailed results.\n\n"
                                    debug_info += f"**Debug Information:**\n"
                                    debug_info += f"- Run ID: {run_id}\n"
                                    debug_info += f"- Task Key: {task_key}\n"
                                    debug_info += f"- Extraction returned: None\n\n"

                                    # Include extraction debug steps if available
                                    if hasattr(self, '_extraction_debug') and self._extraction_debug:
                                        debug_info += f"**Extraction Steps:**\n"
                                        for step in self._extraction_debug:
                                            debug_info += f"- {step}\n"
                                        debug_info += "\n"

                                    debug_info += f"**Troubleshooting:**\n"
                                    debug_info += f"1. Check backend logs/console for '[POWERBI-TOOL]' messages\n"
                                    debug_info += f"2. The notebook should exit with: 'Notebook exited: {{...}}' containing result_data\n"
                                    debug_info += f"3. Verify the task '{task_key}' exists in the multi-task job\n"

                                    return debug_info
                            except Exception as e:
                                logger.error(f"[POWERBI-TOOL] ❌ EXCEPTION during result extraction: {str(e)}", exc_info=True)
                                return f"✅ Task '{task_key}' completed successfully but extraction failed: {str(e)}\n\nRun ID: {run_id}"
                        elif task_status in ["FAILED", "CANCELED", "TIMEDOUT"]:
                            logger.error(f"Task '{task_key}' failed with status: {task_status}")
                            return f"❌ Analysis Failed\n\nTask '{task_key}' status: {task_status}\nRun ID: {run_id}"
                        elif task_status in ["RUNNING", "PENDING", "BLOCKED"]:
                            # Still running, wait and retry
                            logger.info(f"Task '{task_key}' still {task_status}, waiting {poll_interval}s...")
                            time.sleep(poll_interval)
                            elapsed += poll_interval
                        else:
                            logger.warning(f"Unknown task status: {task_status}")
                            time.sleep(poll_interval)
                            elapsed += poll_interval

                    logger.error(f"Task '{task_key}' did not complete within {max_wait} seconds")
                    return f"⏱️ Analysis Timeout\n\nTask '{task_key}' did not complete within {max_wait} seconds.\nRun ID: {run_id}\nLast status: {task_status}"
                else:
                    return f"❌ Failed to extract run ID from result:\n{run_result}"

            else:
                # No job configured - return instructions for setup
                return self._format_setup_instructions(dashboard_id, question_str, job_params)

        except Exception as e:
            logger.error(f"Error executing Power BI analysis: {e}", exc_info=True)
            return f"❌ Error executing analysis: {str(e)}"

    def _extract_run_id(self, result: str) -> Optional[int]:
        """Extract run ID from Databricks job result."""
        try:
            # Look for "Run ID: 12345" pattern
            match = re.search(r'Run ID:\s*(\d+)', result)
            if match:
                return int(match.group(1))
        except Exception as e:
            logger.error(f"Error extracting run ID: {e}")
        return None

    async def _check_task_status(self, databricks_tool, run_id: int, task_key: str = "pbi_e2e_pipeline") -> str:
        """
        Check the status of a specific task in a Databricks job run.

        Args:
            databricks_tool: DatabricksJobsTool instance
            run_id: The run ID to check
            task_key: The task key/name to check status for

        Returns:
            Task status string: SUCCESS, FAILED, RUNNING, PENDING, etc.
        """
        try:
            # Get run details
            run_details_endpoint = f"/api/2.1/jobs/runs/get?run_id={run_id}"
            run_details = await databricks_tool._make_api_call("GET", run_details_endpoint)

            # Check if this is a multi-task job
            tasks = run_details.get('tasks', [])

            if tasks:
                # Multi-task job - find the specific task
                target_task = None
                for task in tasks:
                    if task.get('task_key') == task_key:
                        target_task = task
                        break

                if target_task:
                    # Get the task's state
                    state = target_task.get('state', {})
                    life_cycle_state = state.get('life_cycle_state', 'UNKNOWN')
                    result_state = state.get('result_state', '')

                    # If task has completed, return the result_state (SUCCESS, FAILED, etc.)
                    if life_cycle_state in ['TERMINATED', 'INTERNAL_ERROR']:
                        return result_state if result_state else 'FAILED'
                    else:
                        # Task is still running/pending
                        return life_cycle_state
                else:
                    logger.warning(f"Task '{task_key}' not found in run {run_id}")
                    # Return the main run status as fallback
                    state = run_details.get('state', {})
                    life_cycle_state = state.get('life_cycle_state', 'UNKNOWN')
                    result_state = state.get('result_state', '')
                    return result_state if result_state else life_cycle_state
            else:
                # Single-task job - get the main run status
                state = run_details.get('state', {})
                life_cycle_state = state.get('life_cycle_state', 'UNKNOWN')
                result_state = state.get('result_state', '')

                # If run has completed, return the result_state
                if life_cycle_state in ['TERMINATED', 'INTERNAL_ERROR']:
                    return result_state if result_state else 'FAILED'
                else:
                    return life_cycle_state

        except Exception as e:
            logger.error(f"Error checking task status: {e}", exc_info=True)
            return "ERROR"

    def _format_success_result(self, result: str, semantic_model_id: str, question: str) -> str:
        """Format successful analysis result."""
        output = f"✅ Power BI Analysis Complete\n\n"
        output += f"📊 Semantic Model: {semantic_model_id}\n"
        output += f"❓ Question Analyzed: {question}\n"
        output += f"\n{result}\n"
        return output

    async def _get_notebook_output(self, databricks_tool, run_id: int, task_key: str = "pbi_e2e_pipeline") -> Optional[dict]:
        """
        Extract notebook output from a completed Databricks job run.

        For multi-task jobs, extracts output from the specified task.

        Args:
            databricks_tool: DatabricksJobsTool instance
            run_id: The run ID to get output from
            task_key: The task key/name to get output from (default: "pbi_e2e_pipeline")

        Returns:
            Parsed notebook output as dict, or None if extraction fails
        """
        # Track extraction steps for debugging
        self._extraction_debug = []

        try:
            _debug_log(f"_get_notebook_output: run_id={run_id}, task_key={task_key}")
            self._extraction_debug.append(f"Starting extraction for run_id={run_id}, task_key={task_key}")
            # For multi-task jobs, we need to get the run details first to find the task
            logger.info(f"[POWERBI-TOOL] 🔍 Getting run details for run {run_id}, looking for task '{task_key}'")

            # First, get the run details to see if it's a multi-task job
            run_details_endpoint = f"/api/2.1/jobs/runs/get?run_id={run_id}"
            _debug_log(f"Making API call to: {run_details_endpoint}")
            logger.info(f"[POWERBI-TOOL] Making API call to: {run_details_endpoint}")
            self._extraction_debug.append(f"API call: {run_details_endpoint}")
            run_details = await databricks_tool._make_api_call("GET", run_details_endpoint)

            _debug_log(f"Got run details with {len(run_details)} keys")
            logger.info(f"[POWERBI-TOOL] 📋 Got run details with keys: {list(run_details.keys())}")
            self._extraction_debug.append(f"Run details keys: {list(run_details.keys())}")

            # Check if this is a multi-task job
            tasks = run_details.get('tasks', [])
            _debug_log(f"Found {len(tasks)} tasks in run")
            logger.info(f"[POWERBI-TOOL] 🔢 Found {len(tasks)} tasks in run")
            self._extraction_debug.append(f"Found {len(tasks)} tasks")

            if tasks:
                # Multi-task job - find the specific task
                task_keys = [t.get('task_key') for t in tasks]
                logger.info(f"[POWERBI-TOOL] 🔎 Multi-task job detected. Available tasks: {task_keys}")
                self._extraction_debug.append(f"Available tasks: {task_keys}")

                target_task = None
                for task in tasks:
                    if task.get('task_key') == task_key:
                        target_task = task
                        break

                if target_task:
                    # Get the task's run_id
                    task_run_id = target_task.get('run_id')
                    _debug_log(f"Found task '{task_key}' with run_id={task_run_id}")
                    logger.info(f"[POWERBI-TOOL] ✅ Found task '{task_key}' with run_id {task_run_id}")
                    self._extraction_debug.append(f"Found task '{task_key}' with run_id={task_run_id}")

                    # Get the output for this specific task run
                    task_output_endpoint = f"/api/2.1/jobs/runs/get-output?run_id={task_run_id}"
                    _debug_log(f"Fetching task output from: {task_output_endpoint}")
                    logger.info(f"[POWERBI-TOOL] 📥 Fetching task output from: {task_output_endpoint}")
                    self._extraction_debug.append(f"Fetching output: {task_output_endpoint}")
                    task_response = await databricks_tool._make_api_call("GET", task_output_endpoint)

                    _debug_log(f"Task response has {len(task_response)} keys")
                    logger.info(f"[POWERBI-TOOL] 📦 Task response keys: {list(task_response.keys())}")
                    self._extraction_debug.append(f"Response keys: {list(task_response.keys())}")
                    notebook_output = task_response.get('notebook_output', {})
                    _debug_log(f"Notebook output has {len(notebook_output)} keys")
                    logger.info(f"[POWERBI-TOOL] 📓 Notebook output keys: {list(notebook_output.keys())}")
                    self._extraction_debug.append(f"Notebook output keys: {list(notebook_output.keys())}")
                    result_text = notebook_output.get('result', '')
                    _debug_log(f"Result text length: {len(result_text)} chars, preview: {result_text[:100]}")
                    logger.info(f"[POWERBI-TOOL] 📝 Result text length: {len(result_text)} chars")
                    self._extraction_debug.append(f"Result text length: {len(result_text)} chars")
                else:
                    logger.warning(f"Task '{task_key}' not found. Available tasks: {[t.get('task_key') for t in tasks]}")
                    # Fall back to getting output from the main run (might work for some jobs)
                    output_endpoint = f"/api/2.1/jobs/runs/get-output?run_id={run_id}"
                    response = await databricks_tool._make_api_call("GET", output_endpoint)
                    notebook_output = response.get('notebook_output', {})
                    result_text = notebook_output.get('result', '')
            else:
                # Single-task job - get output directly
                logger.info(f"Single-task job detected, getting output directly")
                output_endpoint = f"/api/2.1/jobs/runs/get-output?run_id={run_id}"
                response = await databricks_tool._make_api_call("GET", output_endpoint)
                notebook_output = response.get('notebook_output', {})
                result_text = notebook_output.get('result', '')

            if not result_text:
                logger.error(f"[POWERBI-TOOL] ❌ No notebook output result found in response")
                self._extraction_debug.append("ERROR: No result_text found")
                return None

            _debug_log(f"Result text preview: {result_text[:200]}")
            logger.info(f"[POWERBI-TOOL] 📄 Notebook output result (first 200 chars): {result_text[:200]}")
            self._extraction_debug.append(f"Result preview: {result_text[:100]}...")

            # Try two parsing strategies:
            # 1. Look for "Notebook exited: {...}" pattern (older format)
            # 2. Parse result_text directly as JSON (current Databricks format from dbutils.notebook.exit)

            json_str = None
            match = re.search(r'Notebook exited:\s*({.+})', result_text, re.DOTALL)

            if match:
                _debug_log("Found 'Notebook exited:' pattern in output")
                logger.info(f"[POWERBI-TOOL] 🎯 Found 'Notebook exited:' pattern in output")
                self._extraction_debug.append("Pattern matched: 'Notebook exited:'")
                json_str = match.group(1)
            else:
                # Try parsing result_text directly as JSON (Databricks dbutils.notebook.exit format)
                _debug_log("No 'Notebook exited:' pattern, trying direct JSON parse")
                logger.info(f"[POWERBI-TOOL] No 'Notebook exited:' pattern found, trying direct JSON parse")
                self._extraction_debug.append("No 'Notebook exited:' pattern - attempting direct JSON parse")
                json_str = result_text.strip()

            if json_str:
                _debug_log(f"Attempting to parse JSON (length: {len(json_str)} chars)")
                logger.info(f"[POWERBI-TOOL] Found JSON in notebook output (length: {len(json_str)} chars)")
                self._extraction_debug.append(f"JSON length: {len(json_str)} chars")

                try:
                    parsed_output = json.loads(json_str)
                    _debug_log(f"Successfully parsed JSON, keys: {list(parsed_output.keys())}")
                    logger.info(f"[POWERBI-TOOL] ✅ Successfully parsed notebook output JSON")
                    logger.info(f"[POWERBI-TOOL] 📊 Parsed output keys: {list(parsed_output.keys())}")
                    self._extraction_debug.append(f"JSON parsed successfully, keys: {list(parsed_output.keys())}")

                    # Extract the actual result data from pipeline_steps.step_3_execution.result_data
                    pipeline_steps = parsed_output.get('pipeline_steps', {})
                    _debug_log(f"Pipeline steps: {list(pipeline_steps.keys())}")
                    logger.info(f"[POWERBI-TOOL] 🔧 Pipeline steps available: {list(pipeline_steps.keys())}")
                    self._extraction_debug.append(f"Pipeline steps: {list(pipeline_steps.keys())}")

                    step_3 = pipeline_steps.get('step_3_execution', {})
                    _debug_log(f"Step 3 keys: {list(step_3.keys())}")
                    logger.info(f"[POWERBI-TOOL] 🎯 Step 3 (execution) keys: {list(step_3.keys())}")
                    self._extraction_debug.append(f"Step 3 keys: {list(step_3.keys())}")

                    result_data = step_3.get('result_data', [])

                    if result_data:
                        _debug_log(f"SUCCESS: Extracted {len(result_data)} result rows")
                        logger.info(f"[POWERBI-TOOL] 🎉 Successfully extracted {len(result_data)} result rows")
                        self._extraction_debug.append(f"SUCCESS: Extracted {len(result_data)} rows")

                        # Build return data
                        return_data = {
                            'status': parsed_output.get('status'),
                            'execution_time': parsed_output.get('execution_time'),
                            'generated_dax': pipeline_steps.get('step_2_dax_generation', {}).get('generated_dax'),
                            'rows_returned': step_3.get('rows_returned', 0),
                            'columns': step_3.get('columns', []),
                            'result_data': result_data
                        }
                        _debug_log(f"Returning data with {len(str(return_data))} chars")
                        return return_data
                    else:
                        logger.error(f"[POWERBI-TOOL] ❌ No result_data found in step_3_execution")
                        logger.error(f"[POWERBI-TOOL] step_3_execution content: {json.dumps(step_3, indent=2)[:500]}")
                        self._extraction_debug.append("ERROR: result_data is empty/missing in step_3_execution")
                        self._extraction_debug.append(f"step_3 content: {json.dumps(step_3, indent=2)[:200]}")
                        return None
                except json.JSONDecodeError as e:
                    logger.error(f"[POWERBI-TOOL] ❌ Failed to parse JSON: {e}")
                    logger.error(f"[POWERBI-TOOL] JSON string (first 500 chars): {json_str[:500]}")
                    self._extraction_debug.append(f"ERROR: JSON parse failed - {str(e)}")
                    return None
            else:
                logger.error(f"[POWERBI-TOOL] ❌ No JSON string to parse")
                logger.error(f"[POWERBI-TOOL] Result text (first 500 chars): {result_text[:500]}")
                self._extraction_debug.append("ERROR: No JSON string extracted from result")
                return None

        except Exception as e:
            logger.error(f"[POWERBI-TOOL] ❌ EXCEPTION in _get_notebook_output: {str(e)}", exc_info=True)
            self._extraction_debug.append(f"EXCEPTION: {str(e)}")
            return None

    def _format_analysis_result(self, semantic_model_id: str, question: str, result_data: dict) -> str:
        """
        Format the extracted analysis results in a nice, readable format.

        Args:
            semantic_model_id: The Power BI semantic model ID
            question: The business question that was analyzed
            result_data: Extracted result data from notebook

        Returns:
            Formatted result string
        """
        output = f"✅ Power BI Analysis Complete\n\n"
        output += f"📊 **Semantic Model**: {semantic_model_id}\n"
        output += f"❓ **Question**: {question}\n\n"

        # Show execution info
        status = result_data.get('status', 'unknown')
        execution_time = result_data.get('execution_time', 'unknown')
        output += f"⏱️ **Execution Time**: {execution_time}\n"
        output += f"✨ **Status**: {status}\n\n"

        # Show the generated DAX query
        generated_dax = result_data.get('generated_dax')
        if generated_dax:
            output += f"📝 **Generated DAX Query**:\n```dax\n{generated_dax}\n```\n\n"

        # Show results summary
        rows_returned = result_data.get('rows_returned', 0)
        columns = result_data.get('columns', [])
        output += f"📈 **Results Summary**:\n"
        output += f"- Rows returned: {rows_returned}\n"
        output += f"- Columns: {', '.join(columns)}\n\n"

        # Show the actual data
        data_rows = result_data.get('result_data', [])
        if data_rows:
            output += f"📊 **Result Data**:\n\n"

            # Format as a table
            output += f"Showing the complete list of data (total: {len(data_rows)}):\n\n"
            output += "```json\n"
            output += json.dumps(data_rows, indent=2)
            output += "\n```\n"
        else:
            output += "⚠️ No result data returned\n"

        return output

    def _format_setup_instructions(self, semantic_model_id: str, question: str, job_params: dict) -> str:
        """Format setup instructions when no Databricks job is configured."""
        output = f"⚙️ Power BI Analysis Setup Required\n\n"
        output += f"To execute Power BI analysis via Databricks, you need to:\n\n"
        output += f"1. **Create a Databricks job** with the Power BI analysis notebook\n"
        output += f"2. **Configure the job ID** in this tool\n\n"
        output += f"**Analysis Parameters:**\n"
        output += f"- Semantic Model: {semantic_model_id}\n"
        output += f"- Question: {question}\n\n"
        output += f"**Job Parameters (JSON):**\n"
        output += f"```json\n{json.dumps(job_params, indent=2)}\n```\n\n"
        output += f"**Notebook Location:**\n"
        output += f"`scripts/dax_analysis_job.py` (from your ask.md guide)\n\n"
        return output
