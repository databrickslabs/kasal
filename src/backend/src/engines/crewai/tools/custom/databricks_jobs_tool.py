import logging
import os
import asyncio
import json
import base64
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union
from datetime import datetime
import time

import aiohttp
from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr, model_validator

logger = logging.getLogger(__name__)


class DatabricksJobsToolSchema(BaseModel):
    """Input schema for DatabricksJobsTool."""
    
    action: str = Field(
        ..., 
        description="Action to perform: 'list' (list all jobs), 'list_my_jobs' (list only your jobs), 'get' (get job details), 'get_notebook' (get notebook content for analysis), 'run' (trigger job run), 'monitor' (get run status), 'create' (create new job)"
    )
    job_id: Optional[int] = Field(
        None, description="Job ID for get, run, or monitor actions"
    )
    run_id: Optional[int] = Field(
        None, description="Run ID for monitoring job execution status"
    )
    job_config: Optional[Dict[str, Any]] = Field(
        None, description="Job configuration for creating new jobs (JSON format)"
    )
    limit: Optional[int] = Field(
        20, description="Maximum number of jobs to list (default: 20)"
    )
    name_filter: Optional[str] = Field(
        None, description="Filter jobs by name or ID substring (case-insensitive). Works with 'list' and 'list_my_jobs' actions"
    )
    job_params: Optional[Union[Dict[str, Any], List[str]]] = Field(
        None, description="Custom parameters to pass when running a job. The tool will automatically wrap dict parameters as {'job_params': '<json_string>'}. Your notebook should read dbutils.widgets.get('job_params') and parse the JSON. Use 'get_notebook' action first to analyze parameters. For Python tasks use list: ['--arg1', 'value1']."
    )
    
    @model_validator(mode='after')
    def validate_input(self) -> 'DatabricksJobsToolSchema':
        """Validate the input parameters based on action."""
        action = self.action.lower()
        
        if action not in ['list', 'list_my_jobs', 'get', 'get_notebook', 'run', 'monitor', 'create']:
            raise ValueError(f"Invalid action '{action}'. Must be one of: list, list_my_jobs, get, get_notebook, run, monitor, create")
        
        if action in ['get', 'get_notebook', 'run'] and not self.job_id:
            raise ValueError(f"job_id is required for action '{action}'")
        
        if action == 'monitor' and not self.run_id:
            raise ValueError("run_id is required for action 'monitor'")
        
        if action == 'create' and not self.job_config:
            raise ValueError("job_config is required for action 'create'")
        
        if action == 'run' and self.job_params:
            # Validate job_params is a dictionary or list
            if not isinstance(self.job_params, (dict, list)):
                raise ValueError("job_params must be a dictionary or list")
        
        return self


class DatabricksJobsTool(BaseTool):
    """
    A tool for managing Databricks Jobs using direct REST API calls.
    
    This tool enables interaction with Databricks Jobs API to:
    - List all jobs in the workspace
    - Get details of a specific job
    - Create new jobs with custom configurations
    - Trigger job runs
    - Monitor job execution status
    - Analyze job notebooks for parameter understanding
    
    Authentication methods supported:
    - OAuth/OBO: User token for on-behalf-of authentication
    - PAT: Personal Access Token from API Keys service
    - Databricks CLI: Environment configuration
    
    All operations use direct REST API calls for optimal performance.
    """
    
    name: str = "Databricks Jobs Manager"
    description: str = (
        "Manage Databricks Jobs using direct REST API calls: list all jobs, list only your jobs, get job details, "
        "analyze job notebooks, create new jobs, trigger job runs with custom parameters, and monitor execution status. "
        "IMPORTANT: Before running a job with parameters, use 'get_notebook' action to analyze what parameters the job expects. "
        "Supports filtering by job name or ID substring with 'name_filter' parameter for 'list' and 'list_my_jobs' actions. "
        "Provide 'action' parameter with values: 'list', 'list_my_jobs', 'get', 'get_notebook', 'run', 'monitor', or 'create'."
    )
    args_schema: Type[BaseModel] = DatabricksJobsToolSchema
    max_usage_count: int = 1  # Limit to 1 usage for 'run' and 'create' actions
    current_usage_count: int = 0
    
    _host: str = PrivateAttr(default=None)
    _token: str = PrivateAttr(default=None)
    _user_token: str = PrivateAttr(default=None)
    _use_oauth: bool = PrivateAttr(default=False)
    _run_executions: Dict[str, str] = PrivateAttr(default_factory=dict)
    _create_executions: Dict[str, str] = PrivateAttr(default_factory=dict)
    
    def __init__(
        self,
        databricks_host: Optional[str] = None,
        tool_config: Optional[dict] = None,
        token_required: bool = True,
        user_token: str = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the DatabricksJobsTool.
        
        Args:
            databricks_host (Optional[str]): Databricks workspace host URL.
            tool_config (Optional[dict]): Tool configuration with auth details.
            token_required (bool): Whether authentication token is required.
            user_token (str): User token for OBO authentication.
            **kwargs: Additional keyword arguments passed to BaseTool.
        """
        super().__init__(**kwargs)
        
        if tool_config is None:
            tool_config = {}
        
        # Set user token for OBO authentication if provided
        if user_token:
            self._user_token = user_token
            self._use_oauth = True
            logger.info("Using user token for OBO authentication")
        
        # Initialize databricks_host from parameter if provided
        initial_databricks_host = databricks_host
        databricks_host = None
        
        # Get configuration from tool_config
        if tool_config:
            # Check if user token is provided in config
            if 'user_token' in tool_config:
                self._user_token = tool_config['user_token']
                self._use_oauth = True
                logger.info("Using user token from tool_config for OBO authentication")
            
            # Check if token is directly provided in config (fallback to PAT)
            if not self._use_oauth:
                if 'DATABRICKS_API_KEY' in tool_config:
                    self._token = tool_config['DATABRICKS_API_KEY']
                    logger.info("Using PAT token from tool_config")
                elif 'token' in tool_config:
                    self._token = tool_config['token']
                    logger.info("Using PAT token from config")
            
            # Handle different possible key formats for host
            if initial_databricks_host:
                databricks_host = initial_databricks_host
                logger.info(f"Using databricks_host from parameter: {databricks_host}")
            else:
                # Check for the uppercase DATABRICKS_HOST (used in tool_factory.py)
                if 'DATABRICKS_HOST' in tool_config:
                    databricks_host = tool_config['DATABRICKS_HOST']
                    logger.info(f"Found DATABRICKS_HOST in config: {databricks_host}")
                # Also check for lowercase databricks_host as a fallback
                elif 'databricks_host' in tool_config:
                    databricks_host = tool_config['databricks_host']
                    logger.info(f"Found databricks_host in config: {databricks_host}")
                else:
                    databricks_host = None
        
        # If no databricks_host from tool_config, use parameter
        if not databricks_host and initial_databricks_host:
            databricks_host = initial_databricks_host
            logger.info(f"Using databricks_host from parameter: {databricks_host}")
        
        # Process host if found in any format
        if databricks_host:
            # Handle if databricks_host is a list
            if isinstance(databricks_host, list) and databricks_host:
                databricks_host = databricks_host[0]
                logger.info(f"Converting databricks_host from list to string: {databricks_host}")
            # Strip https:// and trailing slash if present
            if isinstance(databricks_host, str):
                original_host = databricks_host
                if databricks_host.startswith('https://'):
                    databricks_host = databricks_host[8:]
                    logger.info(f"Stripped https:// prefix from host: {original_host} -> {databricks_host}")
                if databricks_host.startswith('http://'):
                    databricks_host = databricks_host[7:]
                    logger.info(f"Stripped http:// prefix from host: {original_host} -> {databricks_host}")
                if databricks_host.endswith('/'):
                    databricks_host = databricks_host[:-1]
                    logger.info(f"Stripped trailing slash from host")
            
            self._host = databricks_host
            logger.info(f"Final host after processing: {self._host}")
        
        # Try enhanced authentication if not using OAuth
        if not self._use_oauth:
            try:
                # Try to get authentication through enhanced auth system
                from src.utils.databricks_auth import is_databricks_apps_environment
                if is_databricks_apps_environment():
                    self._use_oauth = True
                    logger.info("Detected Databricks Apps environment - using OAuth authentication")
                elif not self._token:
                    # Second fallback: Try to get Databricks API key from API Keys Service
                    logger.info("Attempting to get Databricks API key from API Keys Service...")
                    try:
                        from src.core.unit_of_work import UnitOfWork
                        from src.services.api_keys_service import ApiKeysService
                        import asyncio
                        
                        # Create a new event loop for this sync context
                        loop = None
                        try:
                            loop = asyncio.get_event_loop()
                        except RuntimeError:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                        
                        async def get_databricks_token():
                            async with UnitOfWork() as uow:
                                # Try both possible key names
                                token = await ApiKeysService.get_provider_api_key("databricks") or \
                                       await ApiKeysService.get_provider_api_key("DATABRICKS_API_KEY") or \
                                       await ApiKeysService.get_provider_api_key("DATABRICKS_TOKEN")
                                return token
                        
                        if loop.is_running():
                            # If we're in an async context, we can't run the loop
                            logger.warning("Cannot fetch API key from service in async context - will try environment variables")
                        else:
                            self._token = loop.run_until_complete(get_databricks_token())
                            if self._token:
                                logger.info("✅ Successfully retrieved Databricks API key from API Keys Service")
                            else:
                                logger.warning("❌ No Databricks API key found in API Keys Service")
                                
                    except Exception as api_service_error:
                        logger.warning(f"❌ Failed to get API key from service: {api_service_error}")
                    
                    # Third fallback: environment variables for PAT
                    if not self._token:
                        logger.info("Attempting to get Databricks API key from environment variables...")
                        self._token = os.getenv("DATABRICKS_API_KEY") or os.getenv("DATABRICKS_TOKEN")
                        if self._token:
                            logger.info("✅ Using DATABRICKS_API_KEY/TOKEN from environment")
                        else:
                            logger.warning("❌ No DATABRICKS_API_KEY or DATABRICKS_TOKEN found in environment")
                            
            except ImportError as e:
                logger.debug(f"Enhanced auth not available: {e}")
                # Fall back to API Keys Service and then environment variables
                if not self._token:
                    logger.info("Trying API Keys Service without enhanced auth...")
                    try:
                        from src.core.unit_of_work import UnitOfWork
                        from src.services.api_keys_service import ApiKeysService
                        import asyncio
                        
                        # Create a new event loop for this sync context
                        loop = None
                        try:
                            loop = asyncio.get_event_loop()
                        except RuntimeError:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                        
                        async def get_databricks_token():
                            async with UnitOfWork() as uow:
                                token = await ApiKeysService.get_provider_api_key("databricks") or \
                                       await ApiKeysService.get_provider_api_key("DATABRICKS_API_KEY") or \
                                       await ApiKeysService.get_provider_api_key("DATABRICKS_TOKEN")
                                return token
                        
                        if not loop.is_running():
                            self._token = loop.run_until_complete(get_databricks_token())
                            if self._token:
                                logger.info("✅ Successfully retrieved Databricks API key from API Keys Service (fallback)")
                            else:
                                logger.warning("❌ No Databricks API key found in API Keys Service (fallback)")
                                
                    except Exception as api_service_error:
                        logger.warning(f"❌ Failed to get API key from service (fallback): {api_service_error}")
                    
                    # Final fallback: environment variables
                    if not self._token:
                        logger.info("Final fallback: checking environment variables...")
                        self._token = os.getenv("DATABRICKS_API_KEY") or os.getenv("DATABRICKS_TOKEN")
                        if self._token:
                            logger.info("✅ Using DATABRICKS_API_KEY/TOKEN from environment (final fallback)")
                        else:
                            logger.error("❌ No authentication available: no user token, no API key in service, no environment variables")
        
        # Set fallback values from environment if not set from config
        if not self._host:
            self._host = os.getenv("DATABRICKS_HOST", "your-workspace.cloud.databricks.com")
            logger.info(f"Using host from environment or default: {self._host}")
        
        # Check authentication requirements
        if token_required and not self._use_oauth and not self._token:
            logger.warning("DATABRICKS_API_KEY is required but not provided. Tool will attempt OAuth authentication or return an error when used.")
        
        # Log configuration
        logger.info("DatabricksJobsTool Configuration:")
        logger.info(f"Host: {self._host}")
        logger.info(f"Authentication Method: {'OAuth/OBO' if self._use_oauth else 'PAT'}")
        logger.info(f"Single Execution Control: Enabled (max_usage_count={self.max_usage_count})")
        
        # Log token (masked)
        if self._user_token:
            masked_token = f"{self._user_token[:4]}...{self._user_token[-4:]}" if len(self._user_token) > 8 else "***"
            logger.info(f"User Token (masked): {masked_token}")
        elif self._token:
            masked_token = f"{self._token[:4]}...{self._token[-4:]}" if len(self._token) > 8 else "***"
            logger.info(f"PAT Token (masked): {masked_token}")
        else:
            logger.warning("No token provided - will attempt to use enhanced authentication")

    def reset_execution_state(self) -> str:
        """
        Reset the execution state to allow new runs and creates.
        
        This method clears the execution tracking and resets the usage count.
        Use with caution as it removes protection against duplicate runs and job creation.
        
        Returns:
            str: Confirmation message
        """
        previous_count = self.current_usage_count
        previous_run_executions = len(self._run_executions)
        previous_create_executions = len(self._create_executions)
        total_executions = previous_run_executions + previous_create_executions
        
        self.current_usage_count = 0
        self._run_executions.clear()
        self._create_executions.clear()
        
        logger.info(f"[RESET_EXECUTION_STATE] Cleared {previous_run_executions} run executions and {previous_create_executions} create executions, reset usage count from {previous_count} to 0")
        
        return f"🔄 EXECUTION STATE RESET\n\nCleared tracking for:\n- {previous_run_executions} previous job runs\n- {previous_create_executions} previous job creations\n- Total: {total_executions} executions\n\nUsage count reset from {previous_count} to 0.\n\n⚠️ Single execution protection is now reset - the tool can run and create jobs again."

    async def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests with comprehensive fallback logic."""
        auth_token = None
        auth_method = "unknown"
        
        # First priority: OAuth/OBO with user token
        if self._use_oauth and self._user_token:
            logger.debug("🔐 Attempting OAuth/OBO authentication with user token")
            try:
                from src.utils.databricks_auth import get_databricks_auth_headers
                headers, error = await get_databricks_auth_headers(user_token=self._user_token)
                if not error:
                    logger.info("✅ OAuth/OBO authentication successful")
                    return headers
                else:
                    logger.warning(f"❌ OAuth/OBO authentication failed: {error}")
                    logger.info("🔄 Falling back to direct user token")
                    auth_token = self._user_token
                    auth_method = "user_token_direct"
            except ImportError:
                logger.warning("❌ Enhanced auth module not available")
                logger.info("🔄 Using user token directly")
                auth_token = self._user_token
                auth_method = "user_token_direct"
                
        # Second priority: PAT token from API Keys Service or environment
        elif self._token:
            logger.debug("🔐 Using PAT token authentication")
            auth_token = self._token
            auth_method = "pat_token"
            
        # Last resort: Try to get token from API Keys Service at runtime
        else:
            logger.warning("🚨 No authentication token available, attempting runtime API key retrieval")
            try:
                from src.core.unit_of_work import UnitOfWork
                from src.services.api_keys_service import ApiKeysService
                
                async with UnitOfWork() as uow:
                    runtime_token = await ApiKeysService.get_provider_api_key("databricks") or \
                                   await ApiKeysService.get_provider_api_key("DATABRICKS_API_KEY") or \
                                   await ApiKeysService.get_provider_api_key("DATABRICKS_TOKEN")
                    
                if runtime_token:
                    logger.info("✅ Successfully retrieved token from API Keys Service at runtime")
                    auth_token = runtime_token
                    auth_method = "runtime_api_service"
                else:
                    logger.error("❌ No token found in API Keys Service at runtime")
                    raise Exception("🚨 AUTHENTICATION FAILURE: No authentication token available from any source (user token, PAT, API service)")
                    
            except Exception as e:
                logger.error(f"❌ Runtime API key retrieval failed: {e}")
                raise Exception(f"🚨 AUTHENTICATION FAILURE: Cannot get authentication token - {str(e)}")
        
        if not auth_token:
            logger.error("🚨 CRITICAL: No authentication token available after all fallback attempts")
            raise Exception("🚨 AUTHENTICATION FAILURE: No authentication token available")
        
        # Log the authentication method being used (with masked token)
        masked_token = f"{auth_token[:4]}...{auth_token[-4:]}" if len(auth_token) > 8 else "***"
        logger.info(f"✅ Using authentication method: {auth_method} (token: {masked_token})")
        
        return {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }

    async def _make_api_call(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None,
        timeout: int = 30
    ) -> Dict[str, Any]:
        """
        Make a direct API call to Databricks REST API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., '/api/2.1/jobs/list')
            data: Optional data for POST requests
            timeout: Request timeout in seconds
            
        Returns:
            Dict containing the API response
            
        Raises:
            Exception: If the API call fails
        """
        start_time = time.time()
        
        # Construct full URL
        url = f"https://{self._host}{endpoint}"
        
        # Get authentication headers
        headers = await self._get_auth_headers()
        
        logger.info(f"🌐 Making {method} request to: {url}")
        if data:
            logger.debug(f"📤 Request payload: {json.dumps(data, indent=2)}")
        
        # Log authentication method being used
        auth_header = headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token_preview = auth_header[7:11] + "..." + auth_header[-4:] if len(auth_header) > 15 else "***"
            logger.debug(f"🔐 Using auth token: {token_preview}")
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(
                    method=method,
                    url=url,
                    json=data if data else None,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    api_time = time.time() - start_time
                    response_text = await response.text()
                    
                    logger.info(f"📥 API call completed in {api_time:.3f}s with status {response.status}")
                    
                    if response.status == 200:
                        try:
                            json_response = await response.json()
                            logger.info(f"✅ Successfully parsed JSON response ({len(response_text)} chars)")
                            return json_response
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Failed to parse JSON response: {e}")
                            logger.error(f"Raw response: {response_text[:500]}...")
                            raise Exception(f"Invalid JSON response: {e}")
                    else:
                        # Log detailed error information
                        logger.error(f"❌ API call failed with status {response.status}")
                        logger.error(f"📄 Response headers: {dict(response.headers)}")
                        logger.error(f"📄 Response body: {response_text}")
                        
                        # Check for specific authentication errors
                        if response.status == 401:
                            logger.error("🚨 AUTHENTICATION ERROR: 401 Unauthorized - Token may be invalid or expired")
                        elif response.status == 403:
                            logger.error("🚨 AUTHORIZATION ERROR: 403 Forbidden - Token lacks required permissions")
                        elif response.status == 404:
                            logger.error("🚨 NOT FOUND ERROR: 404 - Resource not found or workspace URL incorrect")
                        
                        error_msg = f"API call failed with status {response.status}: {response_text}"
                        
                        # Try to parse error details if JSON
                        try:
                            error_data = json.loads(response_text)
                            if 'error_code' in error_data:
                                error_msg += f" (Error: {error_data['error_code']})"
                                logger.error(f"🔍 Databricks Error Code: {error_data['error_code']}")
                            if 'message' in error_data:
                                error_msg += f" - {error_data['message']}"
                                logger.error(f"🔍 Databricks Error Message: {error_data['message']}")
                        except json.JSONDecodeError:
                            logger.warning("❌ Could not parse error response as JSON")
                        
                        raise Exception(error_msg)
                        
            except asyncio.TimeoutError:
                api_time = time.time() - start_time
                error_msg = f"API call timed out after {api_time:.3f}s"
                logger.error(error_msg)
                raise Exception(error_msg)

    def _run(self, **kwargs: Any) -> str:
        """
        Execute a Databricks Jobs action using direct REST API calls.
        
        Args:
            action (str): Action to perform (list, get, run, monitor, create)
            job_id (Optional[int]): Job ID for get/run actions
            run_id (Optional[int]): Run ID for monitor action
            job_config (Optional[Dict]): Job configuration for create action
            limit (Optional[int]): Maximum number of jobs to list
            name_filter (Optional[str]): Filter for job names/IDs
            job_params (Optional[Union[Dict, List]]): Parameters for job runs
            
        Returns:
            str: Formatted results of the action
        """
        start_time = time.time()
        
        try:
            # Check if authentication is available
            if not self._use_oauth and not self._token and not self._user_token:
                return "Error: Cannot execute action - no authentication available. Please configure authentication."
            
            # Get and validate parameters
            action = kwargs.get("action", "").lower()
            job_id = kwargs.get("job_id")
            run_id = kwargs.get("run_id")
            job_config = kwargs.get("job_config")
            limit = kwargs.get("limit", 20)
            name_filter = kwargs.get("name_filter")
            job_params = kwargs.get("job_params")
            
            # SINGLE EXECUTION CONTROL: Check for duplicate 'run' and 'create' actions
            if action == "run":
                # Create a unique execution key based on job_id and parameters
                execution_key = f"run_{job_id}_{str(hash(str(job_params))) if job_params else 'no_params'}"
                
                # Check if this exact run has already been executed
                if execution_key in self._run_executions:
                    previous_run_id = self._run_executions[execution_key]
                    logger.warning(f"[SINGLE_EXECUTION] Preventing duplicate run of job {job_id} - already executed with run_id: {previous_run_id}")
                    return f"⚠️ DUPLICATE RUN PREVENTED\n\nJob {job_id} with these parameters has already been executed.\nPrevious run_id: {previous_run_id}\n\n🔒 This tool enforces single execution to prevent duplicate job runs.\n💡 Use action='monitor', run_id={previous_run_id} to check the status of the existing run."
                
                # Check global usage count for run actions
                if self.current_usage_count >= self.max_usage_count:
                    logger.warning(f"[SINGLE_EXECUTION] Max usage count ({self.max_usage_count}) reached for 'run' actions")
                    return f"⚠️ USAGE LIMIT REACHED\n\nThis tool has reached its maximum usage limit of {self.max_usage_count} for 'run' actions.\n🔒 This prevents accidental multiple job executions.\n💡 Create a new tool instance if you need to run additional jobs."
            
            elif action == "create":
                # Create a unique execution key based on job config
                job_name = job_config.get("name", "") if job_config else ""
                execution_key = f"create_{str(hash(str(job_config)))}"
                
                # Check if this exact job config has already been created
                if execution_key in self._create_executions:
                    previous_job_id = self._create_executions[execution_key]
                    logger.warning(f"[SINGLE_EXECUTION] Preventing duplicate creation of job '{job_name}' - already created with job_id: {previous_job_id}")
                    return f"⚠️ DUPLICATE CREATE PREVENTED\n\nA job with this exact configuration has already been created.\nPrevious job_id: {previous_job_id}\nJob name: {job_name}\n\n🔒 This tool enforces single execution to prevent duplicate job creation.\n💡 Use action='get', job_id={previous_job_id} to view the existing job."
                
                # Check global usage count for create actions
                if self.current_usage_count >= self.max_usage_count:
                    logger.warning(f"[SINGLE_EXECUTION] Max usage count ({self.max_usage_count}) reached for 'create' actions")
                    return f"⚠️ USAGE LIMIT REACHED\n\nThis tool has reached its maximum usage limit of {self.max_usage_count} for 'create' actions.\n🔒 This prevents accidental multiple job creations.\n💡 Create a new tool instance if you need to create additional jobs."
            
            # Validate input
            validated_input = DatabricksJobsToolSchema(
                action=action,
                job_id=job_id,
                run_id=run_id,
                job_config=job_config,
                limit=limit,
                name_filter=name_filter,
                job_params=job_params
            )
            
            # Execute the requested action
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                if action == "list":
                    result = loop.run_until_complete(self._list_jobs(limit, name_filter))
                elif action == "list_my_jobs":
                    result = loop.run_until_complete(self._list_my_jobs(limit, name_filter))
                elif action == "get":
                    result = loop.run_until_complete(self._get_job(job_id))
                elif action == "get_notebook":
                    result = loop.run_until_complete(self._get_notebook_content(job_id))
                elif action == "run":
                    result = loop.run_until_complete(self._run_job(job_id, job_params))
                    
                    # SINGLE EXECUTION TRACKING: Track successful run execution
                    if result and "Successfully triggered job" in result:
                        # Extract run_id from the result
                        import re
                        run_id_match = re.search(r'Run ID: (\d+)', result)
                        if run_id_match:
                            new_run_id = run_id_match.group(1)
                            execution_key = f"run_{job_id}_{str(hash(str(job_params))) if job_params else 'no_params'}"
                            self._run_executions[execution_key] = new_run_id
                            self.current_usage_count += 1
                            logger.info(f"[SINGLE_EXECUTION] Tracked successful run: job_id={job_id}, run_id={new_run_id}, usage_count={self.current_usage_count}")
                            
                            # Add execution tracking info to result
                            result += f"\n\n🔒 EXECUTION TRACKING: This tool will prevent duplicate runs of this job with these parameters."
                            result += f"\n📊 Usage Count: {self.current_usage_count}/{self.max_usage_count}"
                        
                elif action == "monitor":
                    result = loop.run_until_complete(self._monitor_run(run_id))
                elif action == "create":
                    result = loop.run_until_complete(self._create_job(job_config))
                    
                    # SINGLE EXECUTION TRACKING: Track successful job creation
                    if result and "Successfully created job" in result:
                        # Extract job_id from the result
                        import re
                        job_id_match = re.search(r'Job ID: (\d+)', result)
                        if job_id_match:
                            new_job_id = job_id_match.group(1)
                            execution_key = f"create_{str(hash(str(job_config)))}"
                            self._create_executions[execution_key] = new_job_id
                            self.current_usage_count += 1
                            logger.info(f"[SINGLE_EXECUTION] Tracked successful creation: job_name={job_config.get('name', 'Unknown')}, job_id={new_job_id}, usage_count={self.current_usage_count}")
                            
                            # Add execution tracking info to result
                            result += f"\n\n🔒 EXECUTION TRACKING: This tool will prevent duplicate creation of jobs with this configuration."
                            result += f"\n📊 Usage Count: {self.current_usage_count}/{self.max_usage_count}"
                else:
                    result = f"Error: Unknown action '{action}'"
            finally:
                loop.close()
            
            total_time = time.time() - start_time
            logger.info(f"Action '{action}' completed in {total_time:.3f}s")
            
            # Add timing info to result if it took more than 2 seconds
            if total_time > 2.0:
                timing_info = f"\n\n⏱️ Performance: Action took {total_time:.1f}s"
                result += timing_info
            
            return result
                
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"Error after {total_time:.3f}s: {str(e)}")
            return f"Error executing Databricks Jobs action: {str(e)}"

    async def _list_jobs(self, limit: int, name_filter: Optional[str] = None) -> str:
        """List all jobs in the workspace with optional name/id filtering."""
        start_time = time.time()
        logger.info(f"[list_jobs] Starting with limit={limit}, filter='{name_filter}'")
        
        try:
            # Make API call to list jobs
            data = {"limit": limit, "expand_tasks": True}
            response = await self._make_api_call("GET", "/api/2.1/jobs/list", data)
            
            jobs = response.get("jobs", [])
            logger.info(f"[list_jobs] Retrieved {len(jobs)} jobs from API")
            
            # Apply name filter if provided
            if name_filter:
                filtered_jobs = []
                filter_lower = name_filter.lower()
                for job in jobs:
                    job_name = job.get("settings", {}).get("name", "").lower()
                    job_id_str = str(job.get("job_id", ""))
                    if filter_lower in job_name or filter_lower in job_id_str:
                        filtered_jobs.append(job)
                jobs = filtered_jobs
                logger.info(f"[list_jobs] Filtered to {len(jobs)} jobs matching '{name_filter}'")
            
            # Format output
            if not jobs:
                return "No jobs found in workspace."
            
            output = f"Found {len(jobs)} jobs:\n"
            output += "=" * 80 + "\n"
            
            for job in jobs:
                job_id = job.get("job_id")
                settings = job.get("settings", {})
                name = settings.get("name", "Unnamed Job")
                creator = job.get("creator_user_name", "Unknown")
                created_time = job.get("created_time")
                
                # Format creation time
                if created_time:
                    try:
                        dt = datetime.fromtimestamp(created_time / 1000)
                        created_str = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        created_str = "Unknown"
                else:
                    created_str = "Unknown"
                
                # Get task info
                tasks = settings.get("tasks", [])
                task_info = f"{len(tasks)} task(s)"
                if tasks:
                    task_types = []
                    for task in tasks:
                        if "notebook_task" in task:
                            task_types.append("Notebook")
                        elif "python_task" in task:
                            task_types.append("Python")
                        elif "sql_task" in task:
                            task_types.append("SQL")
                        else:
                            task_types.append("Other")
                    task_info += f" ({', '.join(set(task_types))})"
                
                output += f"🔧 {name}\n"
                output += f"   ID: {job_id} | Creator: {creator} | Created: {created_str}\n"
                output += f"   Tasks: {task_info}\n"
                
                # Add schedule info if present
                if "schedule" in settings:
                    schedule = settings["schedule"]
                    cron = schedule.get("quartz_cron_expression", "Unknown")
                    output += f"   Schedule: {cron}\n"
                
                output += "\n"
            
            execution_time = time.time() - start_time
            logger.info(f"[list_jobs] Completed in {execution_time:.3f}s")
            
            output += f"\n⏱️ Listed {len(jobs)} jobs in {execution_time:.2f}s"
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[list_jobs] Error after {execution_time:.3f}s: {str(e)}")
            return f"Error listing jobs: {str(e)}"

    async def _list_my_jobs(self, limit: int, name_filter: Optional[str] = None) -> str:
        """List only jobs created by the current user."""
        start_time = time.time()
        logger.info(f"[list_my_jobs] Starting with limit={limit}, filter='{name_filter}'")
        
        try:
            # First get current user info
            current_user = None
            try:
                user_response = await self._make_api_call("GET", "/api/2.0/preview/scim/v2/Me")
                current_user = user_response.get("userName") or user_response.get("emails", [{}])[0].get("value")
                logger.info(f"[list_my_jobs] Current user: {current_user}")
            except:
                logger.warning("[list_my_jobs] Could not determine current user, showing all jobs")
            
            # Get all jobs
            data = {"limit": limit, "expand_tasks": True}
            response = await self._make_api_call("GET", "/api/2.1/jobs/list", data)
            
            jobs = response.get("jobs", [])
            logger.info(f"[list_my_jobs] Retrieved {len(jobs)} total jobs")
            
            # Filter by current user if we have user info
            if current_user:
                my_jobs = []
                for job in jobs:
                    creator = job.get("creator_user_name", "")
                    if creator == current_user:
                        my_jobs.append(job)
                jobs = my_jobs
                logger.info(f"[list_my_jobs] Filtered to {len(jobs)} jobs created by {current_user}")
            
            # Apply name filter if provided
            if name_filter:
                filtered_jobs = []
                filter_lower = name_filter.lower()
                for job in jobs:
                    job_name = job.get("settings", {}).get("name", "").lower()
                    job_id_str = str(job.get("job_id", ""))
                    if filter_lower in job_name or filter_lower in job_id_str:
                        filtered_jobs.append(job)
                jobs = filtered_jobs
                logger.info(f"[list_my_jobs] Further filtered to {len(jobs)} jobs matching '{name_filter}'")
            
            # Format output
            if not jobs:
                user_info = f" created by {current_user}" if current_user else ""
                return f"No jobs found{user_info}."
            
            user_info = f" created by {current_user}" if current_user else ""
            output = f"Found {len(jobs)} jobs{user_info}:\n"
            output += "=" * 80 + "\n"
            
            for job in jobs:
                job_id = job.get("job_id")
                settings = job.get("settings", {})
                name = settings.get("name", "Unnamed Job")
                created_time = job.get("created_time")
                
                # Format creation time
                if created_time:
                    try:
                        dt = datetime.fromtimestamp(created_time / 1000)
                        created_str = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        created_str = "Unknown"
                else:
                    created_str = "Unknown"
                
                # Get task info
                tasks = settings.get("tasks", [])
                task_info = f"{len(tasks)} task(s)"
                if tasks:
                    task_types = []
                    for task in tasks:
                        if "notebook_task" in task:
                            task_types.append("Notebook")
                        elif "python_task" in task:
                            task_types.append("Python")
                        elif "sql_task" in task:
                            task_types.append("SQL")
                        else:
                            task_types.append("Other")
                    task_info += f" ({', '.join(set(task_types))})"
                
                output += f"🔧 {name}\n"
                output += f"   ID: {job_id} | Created: {created_str}\n"
                output += f"   Tasks: {task_info}\n"
                
                # Add schedule info if present
                if "schedule" in settings:
                    schedule = settings["schedule"]
                    cron = schedule.get("quartz_cron_expression", "Unknown")
                    output += f"   Schedule: {cron}\n"
                
                output += "\n"
            
            execution_time = time.time() - start_time
            logger.info(f"[list_my_jobs] Completed in {execution_time:.3f}s")
            
            output += f"\n⏱️ Listed {len(jobs)} jobs in {execution_time:.2f}s"
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[list_my_jobs] Error after {execution_time:.3f}s: {str(e)}")
            return f"Error listing my jobs: {str(e)}"

    async def _get_job(self, job_id: int) -> str:
        """Get details of a specific job."""
        start_time = time.time()
        logger.info(f"[get_job] Getting details for job {job_id}")
        
        try:
            # Get job details
            response = await self._make_api_call("GET", f"/api/2.1/jobs/get?job_id={job_id}")
            
            job_id = response.get("job_id")
            settings = response.get("settings", {})
            name = settings.get("name", "Unnamed Job")
            creator = response.get("creator_user_name", "Unknown")
            created_time = response.get("created_time")
            
            # Format creation time
            if created_time:
                try:
                    dt = datetime.fromtimestamp(created_time / 1000)
                    created_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    created_str = "Unknown"
            else:
                created_str = "Unknown"
            
            # Format detailed output
            output = "Job Details:\n"
            output += "=" * 80 + "\n"
            output += f"🔧 {name}\n"
            output += f"   Job ID: {job_id}\n"
            output += f"   Creator: {creator}\n"
            output += f"   Created: {created_str}\n\n"
            
            # Tasks information
            tasks = settings.get("tasks", [])
            if tasks:
                output += "Tasks:\n"
                for i, task in enumerate(tasks):
                    task_key = task.get("task_key", f"Task_{i}")
                    output += f"  - {task_key}"
                    
                    # Determine task type and details
                    if "notebook_task" in task:
                        notebook_path = task["notebook_task"].get("notebook_path", "Unknown")
                        output += f" (Notebook: {notebook_path})"
                    elif "python_task" in task:
                        python_file = task["python_task"].get("python_file", "Unknown")
                        output += f" (Python: {python_file})"
                    elif "sql_task" in task:
                        warehouse_id = task["sql_task"].get("warehouse_id", "Unknown")
                        output += f" (SQL: warehouse {warehouse_id})"
                    
                    output += "\n"
                output += "\n"
            
            # Cluster information
            job_clusters = settings.get("job_clusters", [])
            if job_clusters:
                output += "Clusters:\n"
                for cluster in job_clusters:
                    cluster_key = cluster.get("job_cluster_key", "Unknown")
                    output += f"  - {cluster_key}: "
                    new_cluster = cluster.get("new_cluster", {})
                    if new_cluster:
                        node_type = new_cluster.get("node_type_id", "Unknown")
                        num_workers = new_cluster.get("num_workers", "Unknown")
                        output += f"{node_type} ({num_workers} workers)"
                    output += "\n"
                output += "\n"
            
            # Schedule information
            schedule = settings.get("schedule")
            if schedule:
                cron = schedule.get("quartz_cron_expression", "Unknown")
                timezone = schedule.get("timezone_id", "UTC")
                output += f"Schedule: {cron} ({timezone})\n\n"
            
            # Get recent runs
            try:
                runs_response = await self._make_api_call("GET", f"/api/2.1/jobs/runs/list?job_id={job_id}&limit=5")
                runs = runs_response.get("runs", [])
                
                if runs:
                    output += "Recent Runs:\n"
                    for run in runs:
                        run_id = run.get("run_id")
                        state = run.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "Unknown")
                        result_state = state.get("result_state", "")
                        run_start_time = run.get("start_time")
                        
                        if run_start_time:
                            try:
                                dt = datetime.fromtimestamp(run_start_time / 1000)
                                start_str = dt.strftime("%Y-%m-%d %H:%M")
                            except:
                                start_str = "Unknown"
                        else:
                            start_str = "Unknown"
                        
                        status_emoji = "🟢" if result_state == "SUCCESS" else "🔴" if result_state == "FAILED" else "🟡"
                        output += f"  {status_emoji} Run {run_id}: {life_cycle_state}"
                        if result_state:
                            output += f" ({result_state})"
                        output += f" - {start_str}\n"
                else:
                    output += "Recent Runs: No runs found\n"
            except:
                output += "Recent Runs: Unable to fetch\n"
            
            execution_time = time.time() - start_time
            logger.info(f"[get_job] Completed in {execution_time:.3f}s")
            
            output += f"\n⏱️ Retrieved job details in {execution_time:.2f}s"
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[get_job] Error after {execution_time:.3f}s: {str(e)}")
            return f"Error getting job details: {str(e)}"

    async def _get_notebook_content(self, job_id: int) -> str:
        """Get notebook content for analysis."""
        start_time = time.time()
        logger.info(f"[get_notebook] Getting notebook content for job {job_id}")
        
        try:
            # First get job details to find notebook paths
            job_response = await self._make_api_call("GET", f"/api/2.1/jobs/get?job_id={job_id}")
            
            settings = job_response.get("settings", {})
            tasks = settings.get("tasks", [])
            
            # Find notebook tasks
            notebook_tasks = []
            for task in tasks:
                if "notebook_task" in task:
                    notebook_tasks.append(task)
            
            if not notebook_tasks:
                return f"Job {job_id} does not contain any notebook tasks. Cannot analyze parameters."
            
            output = f"Notebook Analysis for Job {job_id}:\n"
            output += "=" * 80 + "\n"
            
            for i, task in enumerate(notebook_tasks):
                task_key = task.get("task_key", f"Task_{i}")
                notebook_task = task["notebook_task"]
                notebook_path = notebook_task.get("notebook_path")
                
                output += f"\n📔 Task: {task_key}\n"
                output += f"   Notebook: {notebook_path}\n"
                
                if not notebook_path:
                    output += "   ❌ No notebook path found\n"
                    continue
                
                try:
                    # Export notebook content
                    export_data = {
                        "path": notebook_path,
                        "format": "SOURCE"
                    }
                    
                    export_response = await self._make_api_call(
                        "GET", 
                        f"/api/2.0/workspace/export?path={notebook_path}&format=SOURCE"
                    )
                    
                    # Get content
                    content = export_response.get("content", "")
                    if content:
                        # Decode base64 content
                        try:
                            decoded_content = base64.b64decode(content).decode('utf-8')
                            
                            # Analyze content for parameters
                            output += "   ✅ Notebook content retrieved\n"
                            output += f"   📄 Content length: {len(decoded_content)} characters\n"
                            
                            # Look for parameter patterns
                            param_patterns = []
                            lines = decoded_content.split('\n')
                            
                            # Look for common parameter patterns
                            for line_num, line in enumerate(lines, 1):
                                line_lower = line.lower().strip()
                                
                                # Databricks widgets
                                if 'dbutils.widgets' in line_lower:
                                    param_patterns.append(f"Line {line_num}: {line.strip()}")
                                
                                # getArgument patterns
                                elif 'getargument' in line_lower:
                                    param_patterns.append(f"Line {line_num}: {line.strip()}")
                                
                                # JSON parameter parsing
                                elif any(term in line_lower for term in ['json.loads', 'json.load', 'json.dumps']):
                                    param_patterns.append(f"Line {line_num}: {line.strip()}")
                                
                                # Variable assignments that might be parameters
                                elif any(term in line_lower for term in ['search_id', 'api_key', 'params']):
                                    param_patterns.append(f"Line {line_num}: {line.strip()}")
                            
                            if param_patterns:
                                output += "\n   🔍 Found parameter-related patterns:\n"
                                for pattern in param_patterns[:10]:  # Limit to first 10
                                    output += f"     {pattern}\n"
                                if len(param_patterns) > 10:
                                    output += f"     ... and {len(param_patterns) - 10} more\n"
                            else:
                                output += "\n   ℹ️  No obvious parameter patterns found\n"
                            
                            # Add parameter recommendations
                            output += self._analyze_notebook_parameters(notebook_path, notebook_task)
                            
                        except Exception as decode_error:
                            output += f"   ❌ Failed to decode content: {str(decode_error)}\n"
                    else:
                        output += "   ❌ No content returned from export\n"
                        
                except Exception as export_error:
                    output += f"   ❌ Failed to export notebook: {str(export_error)}\n"
            
            execution_time = time.time() - start_time
            logger.info(f"[get_notebook] Completed in {execution_time:.3f}s")
            
            output += f"\n⏱️ Analyzed notebook(s) in {execution_time:.2f}s"
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[get_notebook] Error after {execution_time:.3f}s: {str(e)}")
            return f"Error getting notebook content: {str(e)}"

    def _analyze_notebook_parameters(self, notebook_path: str, task_config: Dict) -> str:
        """Analyze notebook and provide parameter recommendations."""
        output = "\n    🔍 Parameter Analysis:\n"
        output += "\n    ⚠️  IMPORTANT: The tool will automatically wrap your parameters with key 'job_params' as a JSON string.\n"
        output += "    Your notebook should read this parameter and parse it, e.g.:\n"
        output += "    ```python\n"
        output += "    import json\n"
        output += "    job_params_str = dbutils.widgets.get('job_params')\n"
        output += "    params = json.loads(job_params_str)\n"
        output += "    ```\n"
        
        # Common patterns for search/pagination jobs
        if any(term in notebook_path.lower() for term in ['search', 'gmaps', 'google_maps', 'pagination']):
            output += "    Based on notebook name, this appears to be a search/pagination job.\n"
            output += "    \n"
            output += "    📋 Recommended parameter structure for 'job_params':\n"
            output += "    {\n"
            output += '      "search_id": "unique_identifier",\n'
            output += '      "search_timestamp": "2024-01-01T00:00:00",\n'
            output += '      "api_key": "your_api_key",\n'
            output += '      "max_pages": 5,\n'
            output += '      "search_params": {\n'
            output += '        "query": "search term",\n'
            output += '        "latitude": "47.3769",\n'
            output += '        "longitude": "8.5417",\n'
            output += '        "zoom": "14",\n'
            output += '        "language": "en",\n'
            output += '        "country": "ch"\n'
            output += '      }\n'
            output += '    }\n'
        
        # ETL patterns
        elif any(term in notebook_path.lower() for term in ['etl', 'extract', 'transform', 'load']):
            output += "    Based on notebook name, this appears to be an ETL job.\n"
            output += "    \n"
            output += "    📋 Common ETL parameter structure:\n"
            output += "    {\n"
            output += '      "source_path": "/path/to/source",\n'
            output += '      "target_path": "/path/to/target",\n'
            output += '      "date_range": "2024-01-01,2024-01-31",\n'
            output += '      "batch_size": 1000\n'
            output += '    }\n'
        
        # Generic recommendations
        else:
            output += "    💡 General parameter guidelines:\n"
            output += "    - Use dict format: {'param_name': 'value'} for notebook parameters\n"
            output += "    - Check the notebook code for dbutils.widgets.get() calls\n"
            output += "    - Look for getArgument() or similar parameter retrieval functions\n"
            output += "    - Use 'get_notebook' action to analyze the actual code\n"
        
        return output

    async def _run_job(self, job_id: int, job_params: Optional[Union[Dict, List]] = None) -> str:
        """Trigger a job run."""
        start_time = time.time()
        logger.info(f"[run_job] Triggering job {job_id} with params: {job_params}")
        
        try:
            # Prepare the request payload
            payload = {"job_id": job_id}
            
            # Add job parameters if provided
            if job_params:
                # For Databricks API, we need to pass job_params as a single JSON string
                # with key "job_params" to avoid malformed request errors
                if isinstance(job_params, dict):
                    # Convert the entire dict to a JSON string and pass with single key
                    payload["job_parameters"] = {
                        "job_params": json.dumps(job_params)
                    }
                    logger.info(f"[run_job] Formatted parameters with single key 'job_params': {payload['job_parameters']}")
                elif isinstance(job_params, list):
                    # For list parameters (Python script args), use python_params
                    payload["python_params"] = job_params
                    logger.info(f"[run_job] Using python_params for list: {job_params}")
                else:
                    # Fallback: convert to string with single key
                    payload["job_parameters"] = {
                        "job_params": str(job_params)
                    }
                    logger.info(f"[run_job] Formatted other type as string with key 'job_params': {payload['job_parameters']}")
            
            # Make the API call
            response = await self._make_api_call("POST", "/api/2.1/jobs/run-now", payload)
            
            run_id = response.get("run_id")
            
            if not run_id:
                return f"Error: No run_id returned from API response: {response}"
            
            # Get initial run status
            try:
                run_response = await self._make_api_call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
                
                state = run_response.get("state", {})
                life_cycle_state = state.get("life_cycle_state", "Unknown")
                result_state = state.get("result_state", "")
                
                execution_time = time.time() - start_time
                logger.info(f"[run_job] Successfully started job run {run_id} in {execution_time:.3f}s")
                
                output = f"✅ Successfully triggered job {job_id}\n\n"
                output += f"Run ID: {run_id}\n"
                output += f"Status: {life_cycle_state}"
                if result_state:
                    output += f" ({result_state})"
                output += "\n"
                
                if job_params:
                    output += f"\nParameters passed:\n{json.dumps(job_params, indent=2)}\n"
                
                output += f"\n🚀 Job run started successfully in {execution_time:.2f}s"
                output += f"\n💡 Monitor progress with: action='monitor', run_id={run_id}"
                
                return output
                
            except Exception as status_error:
                # Job was triggered but we couldn't get status
                execution_time = time.time() - start_time
                logger.warning(f"[run_job] Job triggered but status check failed: {status_error}")
                
                output = f"✅ Successfully triggered job {job_id}\n\n"
                output += f"Run ID: {run_id}\n"
                output += f"Status: Unable to check initial status\n"
                
                if job_params:
                    output += f"\nParameters passed:\n{json.dumps(job_params, indent=2)}\n"
                
                output += f"\n🚀 Job run started in {execution_time:.2f}s"
                output += f"\n💡 Monitor progress with: action='monitor', run_id={run_id}"
                
                return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[run_job] Error after {execution_time:.3f}s: {str(e)}")
            return f"Error triggering job run: {str(e)}"

    async def _monitor_run(self, run_id: int) -> str:
        """Monitor a job run status."""
        start_time = time.time()
        logger.info(f"[monitor_run] Monitoring run {run_id}")
        
        try:
            # Get run status
            response = await self._make_api_call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
            
            run_id = response.get("run_id")
            job_id = response.get("job_id")
            state = response.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "Unknown")
            result_state = state.get("result_state", "")
            state_message = state.get("state_message", "")
            
            start_time_ms = response.get("start_time")
            end_time_ms = response.get("end_time")
            
            # Format times
            if start_time_ms:
                try:
                    start_dt = datetime.fromtimestamp(start_time_ms / 1000)
                    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    start_str = "Unknown"
            else:
                start_str = "Not started"
            
            if end_time_ms:
                try:
                    end_dt = datetime.fromtimestamp(end_time_ms / 1000)
                    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Calculate duration
                    if start_time_ms:
                        duration_ms = end_time_ms - start_time_ms
                        duration_str = f"{duration_ms / 1000:.1f}s"
                    else:
                        duration_str = "Unknown"
                except:
                    end_str = "Unknown"
                    duration_str = "Unknown"
            else:
                end_str = "Running"
                duration_str = "In progress"
            
            # Choose emoji based on status
            if result_state == "SUCCESS":
                status_emoji = "✅"
            elif result_state == "FAILED":
                status_emoji = "❌"
            elif life_cycle_state in ["RUNNING", "PENDING"]:
                status_emoji = "🔄"
            else:
                status_emoji = "🟡"
            
            output = f"Run Status for {run_id}:\n"
            output += "=" * 80 + "\n"
            output += f"{status_emoji} Job ID: {job_id}\n"
            output += f"Run ID: {run_id}\n"
            output += f"Status: {life_cycle_state}"
            if result_state:
                output += f" ({result_state})"
            output += "\n"
            
            if state_message:
                output += f"Message: {state_message}\n"
            
            output += f"Started: {start_str}\n"
            output += f"Ended: {end_str}\n"
            output += f"Duration: {duration_str}\n"
            
            # Get task information if available
            tasks = response.get("tasks", [])
            if tasks:
                output += f"\nTasks ({len(tasks)}):\n"
                for task in tasks:
                    task_key = task.get("task_key", "Unknown")
                    task_state = task.get("state", {})
                    task_life_cycle = task_state.get("life_cycle_state", "Unknown")
                    task_result = task_state.get("result_state", "")
                    
                    task_emoji = "✅" if task_result == "SUCCESS" else "❌" if task_result == "FAILED" else "🔄"
                    output += f"  {task_emoji} {task_key}: {task_life_cycle}"
                    if task_result:
                        output += f" ({task_result})"
                    output += "\n"
            
            execution_time = time.time() - start_time
            logger.info(f"[monitor_run] Completed in {execution_time:.3f}s")
            
            output += f"\n⏱️ Status retrieved in {execution_time:.2f}s"
            
            # Add suggestions based on status
            if life_cycle_state in ["RUNNING", "PENDING"]:
                output += f"\n💡 Job is still running. Check again with: action='monitor', run_id={run_id}"
            elif result_state == "FAILED":
                output += f"\n💡 Job failed. Check logs in Databricks UI for job {job_id}, run {run_id}"
            
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[monitor_run] Error after {execution_time:.3f}s: {str(e)}")
            return f"Error monitoring run: {str(e)}"

    async def _create_job(self, job_config: Dict[str, Any]) -> str:
        """Create a new job."""
        start_time = time.time()
        logger.info(f"[create_job] Creating job with config: {job_config}")
        
        try:
            # Validate required fields
            if not job_config.get("name"):
                return "Error: Job configuration must include 'name' field"
            
            if not job_config.get("tasks"):
                return "Error: Job configuration must include 'tasks' field"
            
            # Make the API call
            response = await self._make_api_call("POST", "/api/2.1/jobs/create", job_config)
            
            job_id = response.get("job_id")
            
            if not job_id:
                return f"Error: No job_id returned from API response: {response}"
            
            execution_time = time.time() - start_time
            logger.info(f"[create_job] Successfully created job {job_id} in {execution_time:.3f}s")
            
            output = f"✅ Successfully created job '{job_config['name']}'\n\n"
            output += f"Job ID: {job_id}\n"
            
            # Add task summary
            tasks = job_config.get("tasks", [])
            output += f"Tasks: {len(tasks)} task(s) configured\n"
            
            for i, task in enumerate(tasks):
                task_key = task.get("task_key", f"Task_{i}")
                
                if "notebook_task" in task:
                    notebook_path = task["notebook_task"].get("notebook_path", "Unknown")
                    output += f"  - {task_key}: Notebook ({notebook_path})\n"
                elif "python_task" in task:
                    python_file = task["python_task"].get("python_file", "Unknown")
                    output += f"  - {task_key}: Python ({python_file})\n"
                elif "sql_task" in task:
                    output += f"  - {task_key}: SQL Task\n"
                else:
                    output += f"  - {task_key}: Other\n"
            
            # Add schedule info if present
            if "schedule" in job_config:
                schedule = job_config["schedule"]
                cron = schedule.get("quartz_cron_expression", "Unknown")
                output += f"\nSchedule: {cron}\n"
            
            output += f"\n🚀 Job created successfully in {execution_time:.2f}s"
            output += f"\nNext steps:"
            output += f"\n  - Run now: action='run', job_id={job_id}"
            output += f"\n  - View details: action='get', job_id={job_id}"
            output += f"\n  - Analyze notebook: action='get_notebook', job_id={job_id}"
            
            return output
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[create_job] Error after {execution_time:.3f}s: {str(e)}")
            
            # Provide more specific error messages
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                return f"Error: A job with the name '{job_config.get('name')}' already exists. Please choose a different name."
            elif "permission" in error_msg.lower():
                return f"Error: You don't have permission to create jobs in this workspace."
            elif "invalid" in error_msg.lower():
                return f"Error: Invalid job configuration - {error_msg}"
            else:
                return f"Error creating job: {error_msg}"