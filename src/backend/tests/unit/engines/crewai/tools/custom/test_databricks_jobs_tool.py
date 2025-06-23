import unittest
import asyncio
from unittest.mock import MagicMock, Mock, patch, AsyncMock, call
import json
import aiohttp
import base64
from datetime import datetime
import os

from src.engines.crewai.tools.custom.databricks_jobs_tool import (
    DatabricksJobsTool, 
    DatabricksJobsToolSchema
)


class TestDatabricksJobsToolSchema(unittest.TestCase):
    """Unit tests for DatabricksJobsToolSchema with 100% coverage"""

    def test_valid_list_action(self):
        """Test creating a valid schema for list action"""
        schema = DatabricksJobsToolSchema(
            action="list",
            limit=10
        )
        
        self.assertEqual(schema.action, "list")
        self.assertEqual(schema.limit, 10)
        self.assertIsNone(schema.job_id)
        self.assertIsNone(schema.run_id)
        self.assertIsNone(schema.job_config)

    def test_valid_list_my_jobs_action(self):
        """Test creating a valid schema for list_my_jobs action"""
        schema = DatabricksJobsToolSchema(
            action="list_my_jobs",
            limit=15,
            name_filter="test"
        )
        
        self.assertEqual(schema.action, "list_my_jobs")
        self.assertEqual(schema.limit, 15)
        self.assertEqual(schema.name_filter, "test")

    def test_valid_get_action(self):
        """Test creating a valid schema for get action"""
        schema = DatabricksJobsToolSchema(
            action="get",
            job_id=123
        )
        
        self.assertEqual(schema.action, "get")
        self.assertEqual(schema.job_id, 123)

    def test_valid_get_notebook_action(self):
        """Test creating a valid schema for get_notebook action"""
        schema = DatabricksJobsToolSchema(
            action="get_notebook",
            job_id=456
        )
        
        self.assertEqual(schema.action, "get_notebook")
        self.assertEqual(schema.job_id, 456)

    def test_valid_run_action(self):
        """Test creating a valid schema for run action"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=789,
            job_params={"key": "value"}
        )
        
        self.assertEqual(schema.action, "run")
        self.assertEqual(schema.job_id, 789)
        self.assertEqual(schema.job_params, {"key": "value"})

    def test_valid_monitor_action(self):
        """Test creating a valid schema for monitor action"""
        schema = DatabricksJobsToolSchema(
            action="monitor",
            run_id=999
        )
        
        self.assertEqual(schema.action, "monitor")
        self.assertEqual(schema.run_id, 999)

    def test_valid_create_action(self):
        """Test creating a valid schema for create action"""
        job_config = {
            "name": "Test Job",
            "tasks": [{"task_key": "task1", "notebook_task": {"notebook_path": "/test"}}]
        }
        
        schema = DatabricksJobsToolSchema(
            action="create",
            job_config=job_config
        )
        
        self.assertEqual(schema.action, "create")
        self.assertEqual(schema.job_config, job_config)

    def test_case_insensitive_action(self):
        """Test that action validation is case insensitive"""
        # Test uppercase
        schema = DatabricksJobsToolSchema(action="LIST", limit=5)
        self.assertEqual(schema.action, "LIST")
        
        # Test mixed case
        schema = DatabricksJobsToolSchema(action="LiSt_My_JoBs")
        self.assertEqual(schema.action, "LiSt_My_JoBs")

    def test_invalid_action(self):
        """Test schema validation with invalid action"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="invalid")
        
        self.assertIn("Invalid action 'invalid'", str(cm.exception))

    def test_get_action_missing_job_id(self):
        """Test schema validation for get action without job_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="get")
        
        self.assertIn("job_id is required for action 'get'", str(cm.exception))

    def test_get_notebook_action_missing_job_id(self):
        """Test get_notebook action without job_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="get_notebook")
        
        self.assertIn("job_id is required for action 'get_notebook'", str(cm.exception))

    def test_run_action_missing_job_id(self):
        """Test schema validation for run action without job_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="run")
        
        self.assertIn("job_id is required for action 'run'", str(cm.exception))

    def test_monitor_action_missing_run_id(self):
        """Test schema validation for monitor action without run_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="monitor")
        
        self.assertIn("run_id is required for action 'monitor'", str(cm.exception))

    def test_create_action_missing_job_config(self):
        """Test schema validation for create action without job_config"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="create")
        
        self.assertIn("job_config is required for action 'create'", str(cm.exception))

    def test_schema_default_limit(self):
        """Test schema uses default limit when not specified"""
        schema = DatabricksJobsToolSchema(action="list")
        self.assertEqual(schema.limit, 20)

    def test_job_params_validation_dict(self):
        """Test job_params validation with dict"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params={"param1": "value1", "param2": 123}
        )
        self.assertEqual(schema.job_params, {"param1": "value1", "param2": 123})

    def test_job_params_validation_list(self):
        """Test job_params validation with list"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params=["--arg1", "value1", "--arg2", "value2"]
        )
        self.assertEqual(schema.job_params, ["--arg1", "value1", "--arg2", "value2"])

    def test_job_params_validation_invalid_type(self):
        """Test job_params validation with invalid type"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(
                action="run",
                job_id=123,
                job_params="invalid_string"
            )
        # Pydantic v2 gives a different error message
        self.assertIn("validation error", str(cm.exception).lower())

    def test_job_params_validation_none(self):
        """Test job_params validation with None (should pass)"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params=None
        )
        self.assertIsNone(schema.job_params)

    def test_job_params_validation_empty_dict(self):
        """Test job_params validation with empty dict"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params={}
        )
        self.assertEqual(schema.job_params, {})

    def test_job_params_validation_empty_list(self):
        """Test job_params validation with empty list"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params=[]
        )
        self.assertEqual(schema.job_params, [])

    def test_name_filter_with_list(self):
        """Test name_filter parameter with list action"""
        schema = DatabricksJobsToolSchema(
            action="list",
            name_filter="search_term"
        )
        self.assertEqual(schema.name_filter, "search_term")

    def test_name_filter_with_list_my_jobs(self):
        """Test name_filter parameter with list_my_jobs action"""
        schema = DatabricksJobsToolSchema(
            action="list_my_jobs",
            name_filter="my_job"
        )
        self.assertEqual(schema.name_filter, "my_job")

    def test_all_optional_fields_none(self):
        """Test schema with all optional fields as None"""
        schema = DatabricksJobsToolSchema(
            action="list",
            job_id=None,
            run_id=None,
            job_config=None,
            limit=None,
            name_filter=None,
            job_params=None
        )
        self.assertEqual(schema.action, "list")
        self.assertIsNone(schema.job_id)
        self.assertIsNone(schema.run_id)
        self.assertIsNone(schema.job_config)
        self.assertIsNone(schema.limit)  # Explicitly set to None
        self.assertIsNone(schema.name_filter)
        self.assertIsNone(schema.job_params)


class TestDatabricksJobsTool(unittest.TestCase):
    """Unit tests for DatabricksJobsTool with 100% coverage"""

    def setUp(self):
        """Set up test fixtures"""
        self.tool_config = {
            "DATABRICKS_HOST": "test-workspace.cloud.databricks.com",
            "DATABRICKS_API_KEY": "test-api-key"
        }

    def tearDown(self):
        """Clean up after tests"""
        # Reset any environment variables that might have been set
        for key in ['DATABRICKS_HOST', 'DATABRICKS_API_KEY', 'DATABRICKS_TOKEN']:
            if key in os.environ:
                del os.environ[key]

    def test_tool_initialization_with_config(self):
        """Test DatabricksJobsTool initialization with config"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "test-api-key")
        self.assertFalse(tool._use_oauth)

    def test_tool_initialization_empty_config(self):
        """Test initialization with empty config"""
        tool = DatabricksJobsTool(tool_config={})
        self.assertIsNotNone(tool)

    def test_tool_initialization_none_config(self):
        """Test initialization with None config"""
        tool = DatabricksJobsTool(tool_config=None)
        self.assertIsNotNone(tool)

    def test_tool_initialization_with_parameter(self):
        """Test initialization with databricks_host parameter"""
        tool = DatabricksJobsTool(
            databricks_host="param-workspace.cloud.databricks.com",
            tool_config={"DATABRICKS_API_KEY": "test-key"}
        )
        
        self.assertEqual(tool._host, "param-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "test-key")

    def test_tool_initialization_lowercase_host_key(self):
        """Test initialization with lowercase databricks_host in config"""
        tool = DatabricksJobsTool(
            tool_config={
                "databricks_host": "lowercase-host.databricks.com",
                "DATABRICKS_API_KEY": "test-key"
            }
        )
        
        self.assertEqual(tool._host, "lowercase-host.databricks.com")

    def test_tool_initialization_token_key(self):
        """Test initialization with 'token' key in config"""
        tool = DatabricksJobsTool(
            tool_config={
                "DATABRICKS_HOST": "test.databricks.com",
                "token": "test-token-key"
            }
        )
        
        self.assertEqual(tool._token, "test-token-key")

    def test_oauth_initialization_with_user_token(self):
        """Test OAuth initialization with user_token parameter"""
        tool = DatabricksJobsTool(user_token="user-oauth-token")
        
        self.assertEqual(tool._user_token, "user-oauth-token")
        self.assertTrue(tool._use_oauth)

    def test_oauth_initialization_with_config(self):
        """Test OAuth initialization with config"""
        tool_config = {
            "DATABRICKS_HOST": "test-workspace.cloud.databricks.com",
            "user_token": "test-user-token"
        }
        
        tool = DatabricksJobsTool(tool_config=tool_config)
        
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")
        self.assertEqual(tool._user_token, "test-user-token")
        self.assertTrue(tool._use_oauth)

    def test_host_processing_https(self):
        """Test host URL processing with https prefix"""
        tool = DatabricksJobsTool(databricks_host="https://test-workspace.cloud.databricks.com/")
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")

    def test_host_processing_http(self):
        """Test host URL processing with http prefix"""
        tool = DatabricksJobsTool(databricks_host="http://test-workspace.cloud.databricks.com")
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")

    def test_host_processing_list(self):
        """Test host processing when provided as list"""
        tool_config = {
            "DATABRICKS_HOST": ["workspace1.cloud.databricks.com", "workspace2.cloud.databricks.com"],
            "DATABRICKS_API_KEY": "test-key"
        }
        tool = DatabricksJobsTool(tool_config=tool_config)
        self.assertEqual(tool._host, "workspace1.cloud.databricks.com")

    def test_host_processing_empty_list(self):
        """Test host processing with empty list"""
        tool_config = {
            "DATABRICKS_HOST": [],
            "DATABRICKS_API_KEY": "test-key"
        }
        tool = DatabricksJobsTool(tool_config=tool_config)
        # Should fall back to default
        self.assertEqual(tool._host, "your-workspace.cloud.databricks.com")

    def test_token_masking_short_token(self):
        """Test token masking with short token"""
        tool = DatabricksJobsTool(
            tool_config={
                "DATABRICKS_HOST": "test.com",
                "DATABRICKS_API_KEY": "short"
            }
        )
        self.assertEqual(tool._token, "short")

    def test_user_token_masking_short(self):
        """Test user token masking with short token"""
        tool = DatabricksJobsTool(user_token="short")
        self.assertEqual(tool._user_token, "short")

    @patch.dict('os.environ', {'DATABRICKS_HOST': 'env-workspace.cloud.databricks.com', 'DATABRICKS_API_KEY': 'env-api-key'})
    @patch('src.core.unit_of_work.UnitOfWork')
    def test_environment_variable_fallback(self, mock_uow):
        """Test fallback to environment variables"""
        # Mock the API Keys Service to avoid async issues
        mock_uow.return_value.__aenter__.return_value = MagicMock()
        
        tool = DatabricksJobsTool()
        
        self.assertEqual(tool._host, "env-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "env-api-key")

    @patch.dict('os.environ', {'DATABRICKS_HOST': 'env-workspace.cloud.databricks.com', 'DATABRICKS_TOKEN': 'env-token'})
    @patch('src.core.unit_of_work.UnitOfWork')
    def test_environment_variable_databricks_token(self, mock_uow):
        """Test using DATABRICKS_TOKEN env var"""
        # Mock the API Keys Service to avoid async issues
        mock_uow.return_value.__aenter__.return_value = MagicMock()
        
        tool = DatabricksJobsTool()
        
        self.assertEqual(tool._host, "env-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "env-token")

    def test_databricks_apps_environment_detection(self):
        """Test OAuth is enabled when user_token is provided"""
        tool = DatabricksJobsTool(user_token="test-token")
        
        self.assertTrue(tool._use_oauth)
        self.assertEqual(tool._user_token, "test-token")

    def test_authentication_validation_no_auth(self):
        """Test authentication validation with no auth"""
        tool = DatabricksJobsTool(token_required=True)
        tool._use_oauth = False
        tool._token = None
        tool._user_token = None
        
        result = tool._run(action="list")
        
        self.assertIn("no authentication available", result)

    def test_authentication_validation_with_token_required_false(self):
        """Test with token_required=False"""
        tool = DatabricksJobsTool(token_required=False)
        # Should not show warning
        self.assertIsNotNone(tool)

    def test_invalid_action_error(self):
        """Test handling of invalid actions"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._run(action="invalid_action")
        self.assertIn("Invalid action 'invalid_action'", result)

    def test_run_with_validation_error(self):
        """Test _run with validation error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Missing required job_id for get action
        result = tool._run(action="get")
        self.assertIn("job_id is required", result)

    def test_run_with_exception_in_action(self):
        """Test _run with exception during action execution"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        with patch.object(tool, '_list_jobs', side_effect=Exception("Test error")):
            result = tool._run(action="list")
            self.assertIn("Error executing Databricks Jobs action", result)
            self.assertIn("Test error", result)

    def test_run_with_unknown_action_after_validation(self):
        """Test _run with action that fails validation"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._run(action="fake_action")
        self.assertIn("Error executing Databricks Jobs action", result)
        self.assertIn("Invalid action 'fake_action'", result)

    def test_run_with_timing_over_2_seconds(self):
        """Test that timing info is added when execution takes > 2 seconds"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock a slow operation
        async def slow_list(*args, **kwargs):
            return "Results"
        
        with patch.object(tool, '_list_jobs', new_callable=AsyncMock, return_value="Results"):
            # Mock time.time to simulate > 2 second execution
            with patch('time.time', side_effect=[0, 2.5]):
                result = tool._run(action="list")
                self.assertIn("⏱️ Performance: Action took", result)
                self.assertIn("Results", result)

    def test_run_all_actions(self):
        """Test _run with all valid actions"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock all async methods
        async def mock_async_method(*args, **kwargs):
            return "Mocked result"
        
        with patch.object(tool, '_list_jobs', side_effect=mock_async_method):
            result = tool._run(action="list")
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_list_my_jobs', side_effect=mock_async_method):
            result = tool._run(action="list_my_jobs")
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_get_job', side_effect=mock_async_method):
            result = tool._run(action="get", job_id=123)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_get_notebook_content', side_effect=mock_async_method):
            result = tool._run(action="get_notebook", job_id=123)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_run_job', side_effect=mock_async_method):
            result = tool._run(action="run", job_id=123)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_monitor_run', side_effect=mock_async_method):
            result = tool._run(action="monitor", run_id=456)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_create_job', side_effect=mock_async_method):
            result = tool._run(action="create", job_config={"name": "test"})
            self.assertIn("Mocked result", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_get_auth_headers_with_pat(self, mock_session):
        """Test _get_auth_headers with PAT token"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Run async method
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        self.assertEqual(headers["Authorization"], "Bearer test-api-key")
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_get_auth_headers_with_oauth(self, mock_session):
        """Test _get_auth_headers with OAuth token"""
        tool = DatabricksJobsTool(user_token="oauth-token")
        
        # Run async method
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        self.assertEqual(headers["Authorization"], "Bearer oauth-token")
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_get_auth_headers_with_enhanced_auth_success(self, mock_session, mock_get_auth):
        """Test _get_auth_headers with successful enhanced auth"""
        tool = DatabricksJobsTool(user_token="user-token")
        
        # Mock successful enhanced auth
        async def mock_auth_success(*args, **kwargs):
            return (
                {"Authorization": "Bearer enhanced-token", "Content-Type": "application/json"},
                None
            )
        mock_get_auth.side_effect = mock_auth_success
        
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        self.assertEqual(headers["Authorization"], "Bearer enhanced-token")

    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_get_auth_headers_with_enhanced_auth_failure(self, mock_session, mock_get_auth):
        """Test _get_auth_headers when enhanced auth fails"""
        tool = DatabricksJobsTool(user_token="user-token")
        
        # Mock failed enhanced auth
        async def mock_auth_fail(*args, **kwargs):
            return (None, "Auth error")
        mock_get_auth.side_effect = mock_auth_fail
        
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        # Should fall back to user token
        self.assertEqual(headers["Authorization"], "Bearer user-token")

    def test_get_auth_headers_enhanced_auth_import_error(self):
        """Test _get_auth_headers when enhanced auth module is not available"""
        tool = DatabricksJobsTool(user_token="user-token")
        
        # The tool should handle ImportError internally and fall back to user token
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        # Should use the user token
        self.assertEqual(headers["Authorization"], "Bearer user-token")

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    @patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock)
    @patch('src.core.unit_of_work.UnitOfWork')
    def test_get_auth_headers_no_token_error(self, mock_uow, mock_get_api_key, mock_session):
        """Test _get_auth_headers with no token raises error"""
        tool = DatabricksJobsTool()
        tool._token = None
        tool._user_token = None
        tool._use_oauth = False
        
        # Mock the API Keys Service to return None (no API key found)
        mock_get_api_key.return_value = None
        mock_uow.return_value.__aenter__.return_value = MagicMock()
        
        # Run async method and expect exception
        loop = asyncio.new_event_loop()
        with self.assertRaises(Exception) as cm:
            loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        self.assertIn("No authentication token available", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_success(self, mock_session_class):
        """Test successful API call"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"result": "success"})
        mock_response.text = AsyncMock(return_value='{"result": "success"}')
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        result = asyncio.run(tool._make_api_call("GET", "/api/2.1/jobs/list"))
        
        self.assertEqual(result, {"result": "success"})
        mock_session.request.assert_called_once()

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_with_data(self, mock_session_class):
        """Test API call with data parameter"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"result": "success"})
        mock_response.text = AsyncMock(return_value='{"result": "success"}')
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        test_data = {"job_id": 123}
        
        # Run the async method
        result = asyncio.run(tool._make_api_call("POST", "/api/2.1/jobs/run-now", data=test_data))
        
        self.assertEqual(result, {"result": "success"})
        # Verify data was passed
        call_args = mock_session.request.call_args
        self.assertEqual(call_args[1]['json'], test_data)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_error(self, mock_session_class):
        """Test API call with error response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock error response
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.text = AsyncMock(return_value='{"error_code": "INVALID_REQUEST", "message": "Bad request"}')
        mock_response.headers = {"Content-Type": "application/json"}  # Regular dict, not AsyncMock
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("POST", "/api/2.1/jobs/run-now", {"job_id": 123}))
        
        self.assertIn("API call failed with status 400", str(cm.exception))
        self.assertIn("INVALID_REQUEST", str(cm.exception))
        self.assertIn("Bad request", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_error_invalid_json(self, mock_session_class):
        """Test API call with error response that has invalid JSON"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock error response with invalid JSON
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value='Internal Server Error')
        mock_response.headers = {"Content-Type": "text/plain"}  # Regular dict, not AsyncMock
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.1/jobs/list"))
        
        self.assertIn("API call failed with status 500", str(cm.exception))
        self.assertIn("Internal Server Error", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_timeout(self, mock_session_class):
        """Test API call timeout"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock session that times out
        mock_session = AsyncMock()
        mock_session.request = MagicMock(side_effect=asyncio.TimeoutError())
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.1/jobs/list", timeout=1))
        
        self.assertIn("API call timed out", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_success(self, mock_api_call):
        """Test successful list jobs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {
                        "name": "Test Job 1",
                        "tasks": [{"notebook_task": {"notebook_path": "/test1"}}],
                        "schedule": {"quartz_cron_expression": "0 0 * * *"}
                    },
                    "creator_user_name": "user1@example.com",
                    "created_time": 1640995200000
                },
                {
                    "job_id": 456,
                    "settings": {
                        "name": "Test Job 2",
                        "tasks": [
                            {"python_task": {"python_file": "test.py"}},
                            {"sql_task": {"warehouse_id": "warehouse123"}}
                        ]
                    },
                    "creator_user_name": "user2@example.com",
                    "created_time": 1641081600000
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("Found 2 jobs", result)
        self.assertIn("Test Job 1", result)
        self.assertIn("Test Job 2", result)
        self.assertIn("ID: 123", result)
        self.assertIn("ID: 456", result)
        self.assertIn("Schedule: 0 0 * * *", result)
        # Check for task types in either order since set() doesn't guarantee order
        self.assertTrue(
            "2 task(s) (Python, SQL)" in result or "2 task(s) (SQL, Python)" in result,
            f"Expected task types not found in result: {result}"
        )

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_with_unknown_task_type(self, mock_api_call):
        """Test list jobs with unknown task type"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {
                        "name": "Job with Unknown Task",
                        "tasks": [{"unknown_task": {"some_field": "value"}}]
                    },
                    "creator_user_name": "user@example.com",
                    "created_time": None  # Test None created_time
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("1 task(s) (Other)", result)
        self.assertIn("Created: Unknown", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_with_filter(self, mock_api_call):
        """Test list jobs with name filter"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {"name": "Production Job"},
                    "creator_user_name": "user@example.com",
                    "created_time": 1640995200000
                },
                {
                    "job_id": 456,
                    "settings": {"name": "Test Job"},
                    "creator_user_name": "user@example.com",
                    "created_time": 1640995200000
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10, name_filter="production"))
        
        self.assertIn("Found 1 jobs", result)
        self.assertIn("Production Job", result)
        self.assertNotIn("Test Job", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_filter_by_id(self, mock_api_call):
        """Test list jobs filtering by job ID"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {"name": "Job 123"},
                    "creator_user_name": "user@example.com"
                },
                {
                    "job_id": 456,
                    "settings": {"name": "Job 456"},
                    "creator_user_name": "user@example.com"
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10, name_filter="123"))
        
        self.assertIn("Found 1 jobs", result)
        self.assertIn("Job 123", result)
        self.assertNotIn("Job 456", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_empty(self, mock_api_call):
        """Test list jobs with no results"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {"jobs": []}
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertEqual(result, "No jobs found in workspace.")

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_no_jobs_key(self, mock_api_call):
        """Test list jobs when response has no 'jobs' key"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {}
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertEqual(result, "No jobs found in workspace.")

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_invalid_created_time(self, mock_api_call):
        """Test list jobs with invalid created_time"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {"name": "Job"},
                    "creator_user_name": "user@example.com",
                    "created_time": "invalid"  # Invalid timestamp
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("Created: Unknown", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_error(self, mock_api_call):
        """Test list jobs with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("Error listing jobs: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_success(self, mock_api_call):
        """Test successful list my jobs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call: get current user
            {"userName": "current.user@example.com"},
            # Second call: list all jobs
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "My Job", "tasks": []},
                        "creator_user_name": "current.user@example.com",
                        "created_time": 1640995200000
                    },
                    {
                        "job_id": 456,
                        "settings": {"name": "Other User Job", "tasks": []},
                        "creator_user_name": "other.user@example.com",
                        "created_time": 1640995200000
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Found 1 jobs created by current.user@example.com", result)
        self.assertIn("My Job", result)
        self.assertNotIn("Other User Job", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_with_emails(self, mock_api_call):
        """Test list my jobs when user info has emails instead of userName"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call: get current user with emails
            {"emails": [{"value": "current.user@example.com"}]},
            # Second call: list all jobs
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "My Job"},
                        "creator_user_name": "current.user@example.com"
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Found 1 jobs created by current.user@example.com", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_no_current_user(self, mock_api_call):
        """Test list my jobs when can't determine current user"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call fails
            Exception("Can't get user"),
            # Second call: list all jobs
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "Job 1"},
                        "creator_user_name": "user1@example.com",
                        "created_time": 1640995200000
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Found 1 jobs:", result)
        self.assertIn("Job 1", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_with_filter(self, mock_api_call):
        """Test list my jobs with name filter"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {"userName": "user@example.com"},
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "My Production Job"},
                        "creator_user_name": "user@example.com"
                    },
                    {
                        "job_id": 456,
                        "settings": {"name": "My Test Job"},
                        "creator_user_name": "user@example.com"
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10, name_filter="production"))
        
        self.assertIn("Found 1 jobs", result)
        self.assertIn("My Production Job", result)
        self.assertNotIn("My Test Job", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_no_jobs(self, mock_api_call):
        """Test list my jobs with no jobs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {"userName": "user@example.com"},
            {"jobs": []}
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("No jobs found created by user@example.com", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_error(self, mock_api_call):
        """Test list my jobs with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Error listing my jobs: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_success(self, mock_api_call):
        """Test successful get job details"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {
                    "name": "Detailed Job",
                    "tasks": [
                        {
                            "task_key": "notebook_task",
                            "notebook_task": {"notebook_path": "/path/to/notebook"}
                        },
                        {
                            "task_key": "python_task",
                            "python_task": {"python_file": "script.py"}
                        },
                        {
                            "task_key": "sql_task",
                            "sql_task": {"warehouse_id": "warehouse123"}
                        }
                    ],
                    "job_clusters": [
                        {
                            "job_cluster_key": "cluster1",
                            "new_cluster": {
                                "node_type_id": "i3.xlarge",
                                "num_workers": 2
                            }
                        }
                    ],
                    "schedule": {
                        "quartz_cron_expression": "0 0 * * *",
                        "timezone_id": "UTC"
                    }
                },
                "creator_user_name": "creator@example.com",
                "created_time": 1640995200000
            },
            # Second call: list recent runs
            {
                "runs": [
                    {
                        "run_id": 999,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "SUCCESS"
                        },
                        "start_time": 1641081600000
                    },
                    {
                        "run_id": 998,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED"
                        },
                        "start_time": None  # Test None start_time
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Job Details:", result)
        self.assertIn("Detailed Job", result)
        self.assertIn("Job ID: 123", result)
        self.assertIn("notebook_task (Notebook: /path/to/notebook)", result)
        self.assertIn("python_task (Python: script.py)", result)
        self.assertIn("sql_task (SQL: warehouse warehouse123)", result)
        self.assertIn("cluster1: i3.xlarge (2 workers)", result)
        self.assertIn("Schedule: 0 0 * * * (UTC)", result)
        self.assertIn("🟢 Run 999: TERMINATED (SUCCESS)", result)
        self.assertIn("🔴 Run 998: TERMINATED (FAILED)", result)
        self.assertIn("Unknown", result)  # For run 998 with None start_time

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_no_optional_fields(self, mock_api_call):
        """Test get job with minimal fields"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # Job with no optional fields
            {
                "job_id": 123,
                "settings": {"name": "Simple Job"},
                "creator_user_name": "creator@example.com"
            },
            {"runs": []}
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Simple Job", result)
        self.assertIn("Created: Unknown", result)  # No created_time
        self.assertIn("Recent Runs: No runs found", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_invalid_start_time(self, mock_api_call):
        """Test get job with invalid start time in runs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {"name": "Job"},
                "creator_user_name": "user@example.com"
            },
            {
                "runs": [
                    {
                        "run_id": 999,
                        "state": {"life_cycle_state": "RUNNING"},
                        "start_time": "invalid"  # Invalid timestamp
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("🟡 Run 999: RUNNING", result)
        self.assertIn("Unknown", result)  # Invalid start time

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_runs_error(self, mock_api_call):
        """Test get job when fetching runs fails"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {"name": "Job"},
                "creator_user_name": "creator@example.com"
            },
            # Second call fails
            Exception("Can't get runs")
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Recent Runs: Unable to fetch", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_error(self, mock_api_call):
        """Test get job with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Error getting job details: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_success(self, mock_api_call):
        """Test successful get notebook content"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Create test notebook content
        notebook_content = """# Databricks notebook
dbutils.widgets.text("search_id", "")
search_id = dbutils.widgets.get("search_id")
search_id = getArgument("search_id")

import json
params = json.loads(dbutils.widgets.get("job_params"))
api_key = dbutils.widgets.get("api_key")
"""
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {
                            "task_key": "analyze_task",
                            "notebook_task": {"notebook_path": "/path/to/search_notebook"}
                        }
                    ]
                }
            },
            # Second call: export notebook
            {"content": encoded_content}
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("Notebook Analysis for Job 123", result)
        self.assertIn("analyze_task", result)
        self.assertIn("/path/to/search_notebook", result)
        self.assertIn("✅ Notebook content retrieved", result)
        self.assertIn("Found parameter-related patterns", result)
        self.assertIn("dbutils.widgets", result)
        self.assertIn("getArgument", result)
        self.assertIn("json.loads", result)
        self.assertIn("api_key", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_no_patterns(self, mock_api_call):
        """Test get notebook content with no parameter patterns"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Notebook without parameter patterns
        notebook_content = """# Simple notebook
print("Hello World")
spark.sql("SELECT * FROM table")
"""
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/simple/notebook"}}
                    ]
                }
            },
            {"content": encoded_content}
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("No obvious parameter patterns found", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_many_patterns(self, mock_api_call):
        """Test get notebook content with many parameter patterns"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Create notebook with many patterns
        lines = []
        for i in range(15):
            lines.append(f'dbutils.widgets.get("param{i}")')
        notebook_content = "\n".join(lines)
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/many/params"}}
                    ]
                }
            },
            {"content": encoded_content}
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("... and 5 more", result)  # Should show "and X more"

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_no_notebooks(self, mock_api_call):
        """Test get notebook content with no notebook tasks"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "job_id": 123,
            "settings": {
                "tasks": [
                    {"task_key": "python_task", "python_task": {"python_file": "test.py"}}
                ]
            }
        }
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("does not contain any notebook tasks", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_no_notebook_path(self, mock_api_call):
        """Test get notebook content with missing notebook path"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "job_id": 123,
            "settings": {
                "tasks": [
                    {
                        "task_key": "task1",
                        "notebook_task": {}  # No notebook_path
                    }
                ]
            }
        }
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("❌ No notebook path found", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_empty_content(self, mock_api_call):
        """Test get notebook content with empty content response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/empty/notebook"}}
                    ]
                }
            },
            {"content": ""}  # Empty content
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("❌ No content returned from export", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_decode_error(self, mock_api_call):
        """Test get notebook content with base64 decode error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/bad/notebook"}}
                    ]
                }
            },
            {"content": "invalid_base64!!!"}  # Invalid base64
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("❌ Failed to decode content", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_export_error(self, mock_api_call):
        """Test get notebook content with export error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/path/to/notebook"}}
                    ]
                }
            },
            # Second call fails
            Exception("Export failed")
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("Failed to export notebook: Export failed", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_api_error(self, mock_api_call):
        """Test get notebook content with initial API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("Error getting notebook content: API Error", result)

    def test_analyze_notebook_parameters_search_job(self):
        """Test analyze notebook parameters for search job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/gmaps_search_notebook.py",
            {}
        )
        
        self.assertIn("search/pagination job", result)
        self.assertIn("search_id", result)
        self.assertIn("latitude", result)
        self.assertIn("longitude", result)

    def test_analyze_notebook_parameters_google_maps(self):
        """Test analyze notebook parameters for google maps job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/google_maps_pagination.py",
            {}
        )
        
        self.assertIn("search/pagination job", result)
        self.assertIn("zoom", result)
        self.assertIn("language", result)
        self.assertIn("country", result)

    def test_analyze_notebook_parameters_etl_job(self):
        """Test analyze notebook parameters for ETL job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/etl_transform_notebook.py",
            {}
        )
        
        self.assertIn("ETL job", result)
        self.assertIn("source_path", result)
        self.assertIn("target_path", result)
        self.assertIn("batch_size", result)

    def test_analyze_notebook_parameters_extract_job(self):
        """Test analyze notebook parameters for extract job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/data_extract_job.py",
            {}
        )
        
        self.assertIn("ETL job", result)
        self.assertIn("date_range", result)

    def test_analyze_notebook_parameters_generic(self):
        """Test analyze notebook parameters for generic job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/generic_notebook.py",
            {}
        )
        
        self.assertIn("General parameter guidelines", result)
        self.assertIn("dbutils.widgets.get()", result)
        self.assertIn("getArgument()", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_success(self, mock_api_call):
        """Test successful job run"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: run job
            {"run_id": 456},
            # Second call: get run status
            {
                "state": {
                    "life_cycle_state": "PENDING",
                    "result_state": ""
                }
            }
        ]
        
        result = asyncio.run(tool._run_job(123, {"param1": "value1"}))
        
        self.assertIn("✅ Successfully triggered job 123", result)
        self.assertIn("Run ID: 456", result)
        self.assertIn("Status: PENDING", result)
        self.assertIn("Parameters passed:", result)
        self.assertIn('"param1": "value1"', result)
        self.assertIn("Monitor progress with: action='monitor', run_id=456", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_without_params(self, mock_api_call):
        """Test run job without parameters"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {"run_id": 456},
            {"state": {"life_cycle_state": "RUNNING"}}
        ]
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("✅ Successfully triggered job 123", result)
        self.assertNotIn("Parameters passed:", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_no_run_id(self, mock_api_call):
        """Test run job with no run_id in response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {}
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("Error: No run_id returned", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_status_check_fails(self, mock_api_call):
        """Test run job when status check fails"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: run job
            {"run_id": 456},
            # Second call fails
            Exception("Can't get status")
        ]
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("✅ Successfully triggered job 123", result)
        self.assertIn("Run ID: 456", result)
        self.assertIn("Status: Unable to check initial status", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_error(self, mock_api_call):
        """Test run job with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("Error triggering job run: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_success(self, mock_api_call):
        """Test successful run monitoring"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "Run completed successfully"
            },
            "start_time": 1641081600000,
            "end_time": 1641081900000,
            "tasks": [
                {
                    "task_key": "task1",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS"
                    }
                }
            ]
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("Run Status for 456", result)
        self.assertIn("✅ Job ID: 123", result)
        self.assertIn("Status: TERMINATED (SUCCESS)", result)
        self.assertIn("Message: Run completed successfully", result)
        self.assertIn("Duration: 300.0s", result)
        self.assertIn("✅ task1: TERMINATED (SUCCESS)", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_running(self, mock_api_call):
        """Test monitoring running job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "RUNNING",
                "result_state": ""
            },
            "start_time": 1641081600000
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("🔄 Job ID: 123", result)
        self.assertIn("Status: RUNNING", result)
        self.assertIn("Ended: Running", result)
        self.assertIn("Duration: In progress", result)
        self.assertIn("Job is still running", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_pending(self, mock_api_call):
        """Test monitoring pending job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "PENDING",
                "result_state": ""
            }
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("🔄 Job ID: 123", result)
        self.assertIn("Status: PENDING", result)
        self.assertIn("Started: Not started", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_failed(self, mock_api_call):
        """Test monitoring failed job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Task failed with error"
            },
            "start_time": 1641081600000,
            "end_time": 1641081700000
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("❌ Job ID: 123", result)
        self.assertIn("Status: TERMINATED (FAILED)", result)
        self.assertIn("Message: Task failed with error", result)
        self.assertIn("Job failed. Check logs", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_unknown_state(self, mock_api_call):
        """Test monitoring with unknown state"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "UNKNOWN",
                "result_state": ""
            }
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("🟡 Job ID: 123", result)  # Default emoji
        self.assertIn("Status: UNKNOWN", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_invalid_timestamps(self, mock_api_call):
        """Test monitor run with invalid timestamps"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {"life_cycle_state": "TERMINATED"},
            "start_time": "invalid",
            "end_time": "invalid"
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("Started: Unknown", result)
        self.assertIn("Ended: Unknown", result)
        self.assertIn("Duration: Unknown", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_with_failed_task(self, mock_api_call):
        """Test monitor run with failed tasks"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"},
            "tasks": [
                {
                    "task_key": "task1",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED"
                    }
                },
                {
                    "task_key": "task2",
                    "state": {
                        "life_cycle_state": "RUNNING",
                        "result_state": ""
                    }
                }
            ]
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("❌ task1: TERMINATED (FAILED)", result)
        self.assertIn("🔄 task2: RUNNING", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_error(self, mock_api_call):
        """Test monitor run with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("Error monitoring run: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_success(self, mock_api_call):
        """Test successful job creation"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {"job_id": 789}
        
        job_config = {
            "name": "New Test Job",
            "tasks": [
                {
                    "task_key": "notebook_task",
                    "notebook_task": {"notebook_path": "/test/notebook"}
                },
                {
                    "task_key": "python_task",
                    "python_task": {"python_file": "script.py"}
                },
                {
                    "task_key": "sql_task",
                    "sql_task": {"query": "SELECT * FROM table"}
                },
                {
                    "task_key": "other_task",
                    "other_task_type": {"field": "value"}
                }
            ],
            "schedule": {"quartz_cron_expression": "0 0 * * *"}
        }
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("✅ Successfully created job 'New Test Job'", result)
        self.assertIn("Job ID: 789", result)
        self.assertIn("Tasks: 4 task(s) configured", result)
        self.assertIn("notebook_task: Notebook (/test/notebook)", result)
        self.assertIn("python_task: Python (script.py)", result)
        self.assertIn("sql_task: SQL Task", result)
        self.assertIn("other_task: Other", result)
        self.assertIn("Schedule: 0 0 * * *", result)
        self.assertIn("Run now: action='run', job_id=789", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_minimal(self, mock_api_call):
        """Test create job with minimal configuration"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {"job_id": 789}
        
        job_config = {
            "name": "Minimal Job",
            "tasks": [{"task_key": "task1"}]
        }
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("✅ Successfully created job 'Minimal Job'", result)
        self.assertNotIn("Schedule:", result)  # No schedule

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_missing_name(self, mock_api_call):
        """Test create job with missing name"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        job_config = {"tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: Job configuration must include 'name' field", result)
        mock_api_call.assert_not_called()

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_missing_tasks(self, mock_api_call):
        """Test create job with missing tasks"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        job_config = {"name": "Job Without Tasks"}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: Job configuration must include 'tasks' field", result)
        mock_api_call.assert_not_called()

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_no_job_id(self, mock_api_call):
        """Test create job with no job_id in response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {}
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: No job_id returned", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_already_exists(self, mock_api_call):
        """Test create job when name already exists"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("Job with name 'Test Job' already exists")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: A job with the name 'Test Job' already exists", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_permission_error(self, mock_api_call):
        """Test create job with permission error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("User does not have permission to create jobs")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: You don't have permission to create jobs", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_invalid_config(self, mock_api_call):
        """Test create job with invalid configuration"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("Invalid job configuration: missing required field")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: Invalid job configuration", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_generic_error(self, mock_api_call):
        """Test create job with generic error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("Generic error")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error creating job: Generic error", result)

    def test_tool_description_and_name(self):
        """Test tool has proper name and description"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        self.assertEqual(tool.name, "Databricks Jobs Manager")
        self.assertIn("direct REST API calls", tool.description)
        self.assertIn("list all jobs", tool.description)
        self.assertIn("get_notebook", tool.description)
        self.assertIn("IMPORTANT:", tool.description)

    def test_args_schema(self):
        """Test tool has correct args schema"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        self.assertEqual(tool.args_schema, DatabricksJobsToolSchema)

    def test_all_initialization_paths(self):
        """Test all possible initialization paths"""
        # Test with both user_token and PAT in config (OAuth should take precedence)
        tool = DatabricksJobsTool(
            tool_config={
                "user_token": "oauth-token",
                "DATABRICKS_API_KEY": "pat-token"
            }
        )
        self.assertTrue(tool._use_oauth)
        self.assertEqual(tool._user_token, "oauth-token")
        
        # Test with parameter taking precedence over config
        tool = DatabricksJobsTool(
            databricks_host="param-host.com",
            tool_config={"DATABRICKS_HOST": "config-host.com"}
        )
        self.assertEqual(tool._host, "param-host.com")

    def test_edge_cases(self):
        """Test various edge cases"""
        # Test with None as various parameters
        tool = DatabricksJobsTool(
            databricks_host=None,
            tool_config=None,
            token_required=False,
            user_token=None
        )
        self.assertIsNotNone(tool)
        
        # Test with empty strings
        tool = DatabricksJobsTool(
            databricks_host="",
            tool_config={"DATABRICKS_API_KEY": ""}
        )
        self.assertIsNotNone(tool)


if __name__ == '__main__':
    unittest.main()